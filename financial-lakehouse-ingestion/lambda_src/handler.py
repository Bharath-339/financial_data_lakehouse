from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from ingestion.aws_clients import AwsClients, CheckpointStore, DistributedRateLimiter, RawS3Writer, SecretProvider
from ingestion.errors import ConfigurationError
from ingestion.http import HttpClient
from ingestion.sources import IngestionRuntime, checkpoint_key, dispatch

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(levelname)s %(message)s")
LOGGER = logging.getLogger(__name__)
IST = ZoneInfo("Asia/Kolkata")
_AWS = None
_SECRET_PROVIDER = None


def _get_aws_clients():
    global _AWS, _SECRET_PROVIDER
    if _AWS is None:
        _AWS = AwsClients()
        _SECRET_PROVIDER = SecretProvider(_AWS.secrets)
    return _AWS, _SECRET_PROVIDER


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if not isinstance(event, dict):
        raise ConfigurationError("Lambda event must be a JSON object")

    run_id = str(event.get("run_id") or uuid.uuid4().hex)
    if not run_id or len(run_id) > 80 or any(ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for ch in run_id):
        raise ConfigurationError("run_id may contain only letters, numbers, underscores and hyphens (max 80 characters)")
    extract_date = datetime.now(IST).date().isoformat()
    pk = checkpoint_key(event)
    owner = run_id

    aws, secret_provider = _get_aws_clients()
    checkpoints = CheckpointStore(aws.state_table)
    writer = RawS3Writer(aws.s3, aws.raw_bucket)
    http = HttpClient(
        timeout_seconds=int(os.environ.get("HTTP_TIMEOUT_SECONDS", "20")),
        max_retries=int(os.environ.get("HTTP_MAX_RETRIES", "4")),
    )
    rate_limiter = DistributedRateLimiter(aws.state_table)
    runtime = IngestionRuntime(http=http, writer=writer, secrets=secret_provider, checkpoints=checkpoints, rate_limiter=rate_limiter)

    remaining_ms = context.get_remaining_time_in_millis() if context and hasattr(context, "get_remaining_time_in_millis") else 300_000
    lease_seconds = max(120, int(remaining_ms / 1000) + 60)
    acquired = False
    started_at = datetime.now(timezone.utc).isoformat()
    LOGGER.info("Ingestion started source=%s entity=%s run_id=%s checkpoint=%s",
                event.get("source"), event.get("entity"), run_id, pk)

    try:
        checkpoints.acquire(pk, owner, lease_seconds)
        acquired = True
        state = checkpoints.get(pk)
        result = dispatch(event, runtime, run_id, extract_date, state)
        advance_date = result.get("end_date") if result.get("advance_watermark") else None
        checkpoints.complete(pk, owner, advance_date=advance_date)
        response = {
            "status": "SUCCESS",
            "run_id": run_id,
            "checkpoint_key": pk,
            "source": event.get("source"),
            "entity": event.get("entity"),
            **result,
        }
        try:
            writer.write_audit(run_date=extract_date, source=str(event.get("source", "unknown")),
                               entity=str(event.get("entity", "unknown")), run_id=run_id,
                               manifest={"status": "SUCCESS", "run_id": run_id, "checkpoint_key": pk,
                                         "source": event.get("source"), "entity": event.get("entity"),
                                         "started_at": started_at, "completed_at": datetime.now(timezone.utc).isoformat(),
                                         "record_count": result.get("record_count", 0),
                                         "object_count": len(result.get("object_keys", [])),
                                         "object_keys": result.get("object_keys", []),
                                         "date_range": {"start": result.get("start_date"), "end": result.get("end_date")}})
        except Exception:
            LOGGER.exception("Could not write success audit manifest run_id=%s", run_id)
        LOGGER.info("Ingestion completed %s", json.dumps({
            "run_id": run_id, "source": event.get("source"), "entity": event.get("entity"),
            "record_count": result.get("record_count"), "object_count": len(result.get("object_keys", [])),
        }))
        return response
    except Exception as exc:
        if acquired:
            checkpoints.fail(pk, owner, f"{type(exc).__name__}: {exc}")
            try:
                writer.write_audit(run_date=extract_date, source=str(event.get("source", "unknown")),
                                   entity=str(event.get("entity", "unknown")), run_id=run_id,
                                   manifest={"status": "FAILED", "run_id": run_id, "checkpoint_key": pk,
                                             "source": event.get("source"), "entity": event.get("entity"),
                                             "started_at": started_at, "failed_at": datetime.now(timezone.utc).isoformat(),
                                             "error_type": type(exc).__name__, "error": str(exc)[:1000]})
            except Exception:
                LOGGER.exception("Could not write failure audit manifest run_id=%s", run_id)
        LOGGER.exception("Ingestion failed source=%s entity=%s run_id=%s", event.get("source"), event.get("entity"), run_id)
        raise
