"""Validate immutable Bronze envelopes, quarantine invalid object references and audit results."""
from __future__ import annotations

import gzip
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

REQUIRED_METADATA = ("source", "entity", "entity_key", "run_id", "extract_date", "ingested_at", "request")
SUPPORTED_ENTITIES = {
    ("upstox", "equity_ohlcv"),
    ("upstox", "instrument_master"),
    ("coingecko", "market_snapshot"),
    ("frankfurter", "daily_rates"),
    ("rbi", "table_catalogue"),
    ("rbi", "table_metadata"),
    ("rbi", "table_rows"),
}


def validate_envelope(raw: bytes, *, expected_source: str | None = None,
                      expected_entity: str | None = None) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    try:
        body = gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw
        envelope = json.loads(body.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return None, [f"invalid_json_or_gzip:{type(exc).__name__}"]

    if not isinstance(envelope, dict):
        return None, ["envelope_not_object"]

    metadata = envelope.get("metadata")
    if not isinstance(metadata, dict):
        return envelope, ["metadata_missing_or_not_object"]

    for field in REQUIRED_METADATA:
        if field not in metadata or metadata[field] is None:
            errors.append(f"metadata_{field}_missing")
    if metadata.get("request") is not None and not isinstance(metadata.get("request"), dict):
        errors.append("metadata_request_not_object")
    for field in ("source", "entity", "entity_key", "run_id", "extract_date", "ingested_at"):
        value = metadata.get(field)
        if value is not None and not isinstance(value, str):
            errors.append(f"metadata_{field}_must_be_string")

    if "response" not in envelope:
        errors.append("response_missing")
    elif envelope.get("response") is None:
        errors.append("response_null")

    source = metadata.get("source")
    entity = metadata.get("entity")
    if source and entity and (source, entity) not in SUPPORTED_ENTITIES:
        errors.append("unsupported_source_entity")
    if expected_source and source != expected_source:
        errors.append("source_path_mismatch")
    if expected_entity and entity != expected_entity:
        errors.append("entity_path_mismatch")

    return envelope, errors


def validate_prefix(*, s3, bucket: str, source: str, entity: str, run_date: str | None = None,
                    run_id_filter: str | None = None, fail_on_invalid: bool = True) -> dict[str, Any]:
    prefix = f"bronze/{source}/{entity}/"
    if run_date:
        prefix += f"extract_date={run_date}/"

    report_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    objects: list[str] = []
    invalid_records: list[dict[str, Any]] = []
    valid_count = invalid_count = total_bytes = 0

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            # The S3 layout nests run_id below extract_date. Filter by key rather than
            # appending run_id directly to the prefix so run_id-only validation works.
            if run_id_filter and f"run_id={run_id_filter}/" not in key:
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            total_bytes += len(body)
            objects.append(key)
            _envelope, errors = validate_envelope(body, expected_source=source, expected_entity=entity)
            if errors:
                invalid_count += 1
                invalid_records.append({"source_key": key, "errors": errors, "size_bytes": len(body)})
            else:
                valid_count += 1

    report = {
        "validation_run_id": report_id,
        "status": "FAILED" if invalid_count and fail_on_invalid else "SUCCESS",
        "source": source,
        "entity": entity,
        "prefix": prefix,
        "validated_at": now.isoformat(),
        "object_count": len(objects),
        "valid_object_count": valid_count,
        "invalid_object_count": invalid_count,
        "total_bytes": total_bytes,
    }
    if invalid_records:
        quarantine_key = (
            f"quarantine/bronze_validation/run_date={run_date or now.date().isoformat()}"
            f"/validation_run_id={report_id}/rejected_objects.json"
        )
        s3.put_object(Bucket=bucket, Key=quarantine_key,
                      Body=json.dumps(invalid_records, separators=(",", ":")).encode("utf-8"),
                      ContentType="application/json")
        report["quarantine_key"] = quarantine_key

    audit_key = (
        f"audit/bronze_validation/run_date={run_date or now.date().isoformat()}"
        f"/{source}/{entity}/{report_id}.json"
    )
    s3.put_object(Bucket=bucket, Key=audit_key,
                  Body=json.dumps(report, separators=(",", ":")).encode("utf-8"),
                  ContentType="application/json")
    report["audit_key"] = audit_key

    if invalid_count and fail_on_invalid:
        raise ValueError(
            f"Bronze validation failed: {invalid_count} invalid object(s); "
            f"audit={audit_key}; quarantine={report.get('quarantine_key')}"
        )
    if not objects:
        raise ValueError(f"No Bronze objects found for prefix s3://{bucket}/{prefix}")
    return report


def main() -> None:
    import argparse
    import boto3
    from botocore.config import Config

    parser = argparse.ArgumentParser(description="Validate financial lakehouse Bronze envelopes")
    parser.add_argument("--bucket", default=os.environ.get("RAW_BUCKET"), required=False)
    parser.add_argument("--source", required=True)
    parser.add_argument("--entity", required=True)
    parser.add_argument("--run-date")
    parser.add_argument("--run-id")
    parser.add_argument("--allow-invalid", action="store_true")
    args = parser.parse_args()
    if not args.bucket:
        parser.error("--bucket or RAW_BUCKET is required")

    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("FLOCI_ENDPOINT_URL") or None
    region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    config = Config(s3={"addressing_style": "path"}) if endpoint else None
    client = boto3.client("s3", region_name=region, endpoint_url=endpoint, config=config)
    report = validate_prefix(s3=client, bucket=args.bucket, source=args.source, entity=args.entity,
                             run_date=args.run_date, run_id_filter=args.run_id,
                             fail_on_invalid=not args.allow_invalid)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
