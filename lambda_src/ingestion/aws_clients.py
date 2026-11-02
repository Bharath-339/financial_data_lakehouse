from __future__ import annotations

import gzip
import json
import logging
import os
import time
import random
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .errors import ConcurrentInvocationError, ConfigurationError

LOGGER = logging.getLogger(__name__)


class AwsClients:
    def __init__(self):
        region = os.environ.get("AWS_REGION")
        # Floci/local AWS emulators expose one AWS-compatible endpoint. In AWS this
        # remains unset and boto3 uses the normal regional service endpoints.
        endpoint_url = os.environ.get("AWS_ENDPOINT_URL") or None
        s3_config = Config(s3={"addressing_style": "path"}) if endpoint_url else None
        self.s3 = boto3.client("s3", region_name=region, endpoint_url=endpoint_url, config=s3_config)
        self.secrets = boto3.client("secretsmanager", region_name=region, endpoint_url=endpoint_url)
        self.dynamodb = boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)
        table_name = os.environ.get("STATE_TABLE")
        bucket_name = os.environ.get("RAW_BUCKET")
        if not table_name or not bucket_name:
            raise ConfigurationError("STATE_TABLE and RAW_BUCKET environment variables are required")
        self.state_table = self.dynamodb.Table(table_name)
        self.raw_bucket = bucket_name


class SecretProvider:
    """Fetches JSON secrets. Secrets are cached briefly within a warm Lambda runtime."""

    def __init__(self, client, cache_ttl_seconds: int = 60):
        self.client = client
        self.cache_ttl_seconds = cache_ttl_seconds
        self._cache: dict[str, tuple[float, dict[str, Any]]] = {}

    def get_json(self, secret_arn: str, required: bool = True) -> dict[str, Any]:
        if not secret_arn:
            if required:
                raise ConfigurationError("The required secret ARN is not configured")
            return {}

        now = time.time()
        cached = self._cache.get(secret_arn)
        if cached and now - cached[0] < self.cache_ttl_seconds:
            return cached[1]

        response = self.client.get_secret_value(SecretId=secret_arn)
        raw = response.get("SecretString")
        if not raw:
            raise ConfigurationError(f"Secret {secret_arn} has no SecretString")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigurationError(f"Secret {secret_arn} must contain a JSON object") from exc
        if not isinstance(value, dict):
            raise ConfigurationError(f"Secret {secret_arn} must contain a JSON object")
        self._cache[secret_arn] = (now, value)
        return value


class RawS3Writer:
    def __init__(self, s3_client, bucket: str):
        self.s3 = s3_client
        self.bucket = bucket

    def write(
        self,
        *,
        source: str,
        entity: str,
        entity_key: str,
        run_id: str,
        extract_date: str,
        page_number: int,
        request_metadata: dict[str, Any],
        response_payload: Any,
    ) -> tuple[str, int]:
        safe_entity_key = self._safe_key(entity_key)
        envelope = {
            "metadata": {
                "source": source,
                "entity": entity,
                "entity_key": entity_key,
                "run_id": run_id,
                "extract_date": extract_date,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "request": request_metadata,
            },
            "response": response_payload,
        }
        raw = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
        compressed = gzip.compress(raw)
        key = (
            f"bronze/{source}/{entity}/extract_date={extract_date}/"
            f"run_id={run_id}/{safe_entity_key}/part-{page_number:05d}.json.gz"
        )
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compressed,
            ContentType="application/json",
            ContentEncoding="gzip",
            ServerSideEncryption="AES256",
            Metadata={
                "source": source,
                "entity": entity,
                "run-id": run_id,
                "page-number": str(page_number),
            },
        )
        LOGGER.info("Bronze object written key=%s bytes=%s", key, len(compressed))
        return key, len(compressed)


    def write_audit(self, *, run_date: str, source: str, entity: str, run_id: str, manifest: dict[str, Any]) -> str:
        source_key = self._safe_key(source)
        entity_key = self._safe_key(entity)
        key = f"audit/ingestion_runs/run_date={run_date}/source={source_key}/entity={entity_key}/{run_id}.json"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(manifest, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )
        return key

    @staticmethod
    def _safe_key(value: str) -> str:
        cleaned = "".join(ch if ch.isalnum() or ch in "-_.=" else "_" for ch in value)
        return cleaned[:180] or "unknown"


class DistributedRateLimiter:
    """Coordinates request spacing across concurrent Lambda execution environments."""

    def __init__(self, table, max_contention_retries: int = 100):
        self.table = table
        self.max_contention_retries = max_contention_retries

    def wait(self, provider: str, min_interval_seconds: float) -> None:
        if min_interval_seconds <= 0:
            return
        pk = f"RATE#{provider.upper()}"
        interval_ms = max(1, int(min_interval_seconds * 1000))
        for attempt in range(self.max_contention_retries):
            response = self.table.get_item(Key={"pk": pk}, ConsistentRead=True)
            item = response.get("Item", {})
            previous = item.get("next_allowed_ms")
            now_ms = int(time.time() * 1000)
            slot_ms = max(now_ms, int(previous)) if previous is not None else now_ms
            next_allowed_ms = slot_ms + interval_ms
            try:
                if previous is None:
                    condition = "attribute_not_exists(next_allowed_ms)"
                    values = {":next": next_allowed_ms, ":now": now_ms}
                else:
                    condition = "next_allowed_ms = :previous"
                    values = {":previous": int(previous), ":next": next_allowed_ms, ":now": now_ms}
                self.table.update_item(
                    Key={"pk": pk},
                    UpdateExpression="SET next_allowed_ms = :next, updated_at = :now",
                    ConditionExpression=condition,
                    ExpressionAttributeValues=values,
                )
                delay_ms = max(0, slot_ms - now_ms)
                if delay_ms:
                    time.sleep(delay_ms / 1000.0)
                return
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                    raise
                time.sleep(min(0.05, 0.005 * (attempt + 1)) + random.uniform(0, 0.005))
        raise RuntimeError(f"Could not reserve a request slot for provider {provider} after contention retries")


class CheckpointStore:
    def __init__(self, table):
        self.table = table

    def get(self, pk: str) -> dict[str, Any]:
        response = self.table.get_item(Key={"pk": pk}, ConsistentRead=True)
        return response.get("Item", {})

    def acquire(self, pk: str, owner: str, lease_seconds: int) -> None:
        now = int(time.time())
        try:
            self.table.update_item(
                Key={"pk": pk},
                UpdateExpression="SET lock_owner = :owner, lease_until = :lease, updated_at = :now, #status = :running",
                ConditionExpression="attribute_not_exists(lease_until) OR lease_until < :now",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":owner": owner,
                    ":lease": now + lease_seconds,
                    ":now": now,
                    ":running": "IN_PROGRESS",
                },
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise ConcurrentInvocationError(f"An invocation already holds checkpoint {pk}") from exc
            raise

    def complete(self, pk: str, owner: str, *, advance_date: str | None = None) -> None:
        now = int(time.time())
        names = {"#status": "status"}
        values: dict[str, Any] = {
            ":owner": owner,
            ":now": now,
            ":success": "SUCCESS",
        }
        update = "SET #status = :success, last_successful_at = :now, last_run_id = :owner, updated_at = :now REMOVE lock_owner, lease_until, last_error"
        if advance_date:
            previous = self.get(pk).get("last_successful_date")
            latest = max(previous, advance_date) if previous else advance_date
            update = update.replace("SET #status", "SET last_successful_date = :date, #status")
            values[":date"] = latest
        self.table.update_item(
            Key={"pk": pk},
            UpdateExpression=update,
            ConditionExpression="lock_owner = :owner",
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )

    def fail(self, pk: str, owner: str, error: str) -> None:
        try:
            self.table.update_item(
                Key={"pk": pk},
                UpdateExpression="SET #status = :failed, last_error = :error, last_failed_run_id = :owner, updated_at = :now REMOVE lock_owner, lease_until",
                ConditionExpression="lock_owner = :owner",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":owner": owner,
                    ":failed": "FAILED",
                    ":error": error[:1000],
                    ":now": int(time.time()),
                },
            )
        except ClientError as exc:
            LOGGER.warning("Could not release checkpoint lock pk=%s error=%s", pk, exc.response.get("Error", {}).get("Code"))
