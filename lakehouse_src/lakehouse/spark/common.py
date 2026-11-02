"""Shared Spark, Iceberg, Floci and AWS runtime helpers."""
from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from typing import Any


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Financial Data Lakehouse Spark job")
    parser.add_argument("--bucket", default=os.environ.get("RAW_BUCKET"))
    parser.add_argument("--warehouse", default=os.environ.get("WAREHOUSE_URI"))
    parser.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "glue_catalog"))
    parser.add_argument("--silver-database", default=os.environ.get("SILVER_DATABASE", "lakehouse_silver"))
    parser.add_argument("--gold-database", default=os.environ.get("GOLD_DATABASE", "lakehouse_gold"))
    parser.add_argument("--source")
    parser.add_argument("--entity")
    parser.add_argument("--extract-date")
    parser.add_argument("--run-id")
    parser.add_argument("--job-run-id")
    parser.add_argument("--reference-dir", default=os.environ.get("REFERENCE_DIR"))
    parser.add_argument("--mode", choices=("incremental", "full"), default="incremental")
    parser.add_argument("--local", action="store_true", default=os.environ.get("LAKEHOUSE_ENV", "").lower() == "local")
    args, _unknown = parser.parse_known_args(argv)  # AWS Glue supplies its own --JOB_NAME and bookmark arguments.
    if not args.bucket:
        parser.error("--bucket or RAW_BUCKET is required")
    args.warehouse = args.warehouse or f"s3://{args.bucket}/warehouse/"
    args.reference_dir = args.reference_dir or f"s3a://{args.bucket}/reference"
    return args


def _sql_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def create_spark(app_name: str, args: argparse.Namespace):
    """Create a Spark session using the same GlueCatalog in local and AWS modes.

    Local mode expects Spark 3.5.x, Iceberg 1.10.x, hadoop-aws matching the
    installed Spark Hadoop version, and Floci at AWS_ENDPOINT_URL.
    AWS Glue mode uses the Iceberg runtime supplied by the selected Glue version.
    """
    from pyspark.sql import SparkSession

    catalog = _sql_identifier(args.catalog)
    builder = SparkSession.builder.appName(app_name)
    builder = (builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.warehouse", args.warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))

    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("FLOCI_ENDPOINT_URL")
    region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    if endpoint:
        access_key = os.environ.get("AWS_ACCESS_KEY_ID", "flociadmin")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "flociadmin")
        builder = (builder
            .config(f"spark.sql.catalog.{catalog}.glue.endpoint", endpoint)
            .config(f"spark.sql.catalog.{catalog}.client.region", region)
            .config(f"spark.sql.catalog.{catalog}.s3.endpoint", endpoint)
            .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
            .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", access_key)
            .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", secret_key)
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.endpoint.region", region)
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"))

    return builder.getOrCreate()


def table_name(catalog: str, database: str, table: str) -> str:
    return ".".join(_sql_identifier(part) for part in (catalog, database, table))


def ensure_database(spark, catalog: str, database: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_sql_identifier(catalog)}.{_sql_identifier(database)}")


def _spark_sql_type(data_type) -> str:
    """Return a Spark SQL type for additive Iceberg schema evolution."""
    value = data_type.simpleString()
    # Spark's simpleString types are generated from the in-memory schema, not user input.
    if not re.fullmatch(r"[A-Za-z0-9_(),<> ]+", value):
        raise ValueError(f"Unsupported Spark type in schema evolution: {value}")
    return value


def _ensure_additive_schema(spark, df, target: str) -> None:
    existing = {field.name.lower(): field.dataType for field in spark.table(target).schema.fields}
    for field in df.schema.fields:
        old_type = existing.get(field.name.lower())
        if old_type is None:
            spark.sql(f"ALTER TABLE {target} ADD COLUMNS (`{field.name}` {_spark_sql_type(field.dataType)})")
        elif old_type != field.dataType:
            raise TypeError(
                f"Incompatible schema change for {target}.{field.name}: "
                f"existing={old_type.simpleString()} incoming={field.dataType.simpleString()}. "
                "Add an explicit migration/cast rather than silently changing a financial column."
            )


def merge_iceberg(
    spark,
    df,
    *,
    target: str,
    keys: list[str],
    partition_transform: str | None = None,
    freshness_column: str = "ingested_at",
) -> None:
    """Create an Iceberg table when absent, then idempotently upsert incoming rows.

    The source DataFrame must already be deduplicated to one row per natural key.
    Schema evolution is additive only; incompatible type changes fail loudly.
    """
    if not keys:
        raise ValueError("At least one MERGE key is required")
    missing = [key for key in keys if key not in df.columns]
    if missing:
        raise ValueError(f"MERGE source for {target} is missing key columns: {missing}")
    if df.limit(1).count() == 0:
        return

    target_parts = target.split(".")
    if len(target_parts) != 3:
        raise ValueError(f"Target must be catalog.database.table: {target}")
    for part in target_parts:
        _sql_identifier(part)

    view = "_incoming_" + "_".join(target_parts)
    df.createOrReplaceTempView(view)
    try:
        partition_clause = f" PARTITIONED BY ({partition_transform})" if partition_transform else ""
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {target} USING iceberg{partition_clause} "
            f"AS SELECT * FROM `{view}` WHERE 1 = 0"
        )
        _ensure_additive_schema(spark, df, target)

        columns = df.columns
        quoted_keys = [f"t.`{key}` = s.`{key}`" for key in keys]
        on_clause = " AND ".join(quoted_keys)
        update_columns = [column for column in columns if column not in keys]
        update_set = ", ".join(f"t.`{column}` = s.`{column}`" for column in update_columns)
        insert_columns = ", ".join(f"`{column}`" for column in columns)
        insert_values = ", ".join(f"s.`{column}`" for column in columns)

        matched_clause = ""
        if update_columns:
            if freshness_column in columns:
                matched_clause = (
                    f"WHEN MATCHED AND (t.`{freshness_column}` IS NULL OR "
                    f"s.`{freshness_column}` >= t.`{freshness_column}`) "
                    f"THEN UPDATE SET {update_set}"
                )
            else:
                matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
        spark.sql(
            f"MERGE INTO {target} t USING `{view}` s ON {on_clause} "
            f"{matched_clause} WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )
    finally:
        spark.catalog.dropTempView(view)


def write_json_audit(bucket: str, key: str, payload: dict[str, Any]) -> None:
    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("FLOCI_ENDPOINT_URL") or None
    region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))
    config = Config(s3={"addressing_style": "path"}) if endpoint else None
    s3 = boto3.client("s3", region_name=region, endpoint_url=endpoint, config=config)
    body = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")


def run_metadata(args: argparse.Namespace, job: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    return {
        "job": job,
        "job_run_id": args.job_run_id or now.strftime("%Y%m%dT%H%M%S%fZ"),
        "started_at": now.isoformat(),
        "mode": args.mode,
        "environment": "local" if args.local or os.environ.get("AWS_ENDPOINT_URL") else "aws",
    }


def path_for_bronze(bucket: str, source: str, entity: str) -> str:
    if not re.fullmatch(r"[a-z0-9_-]+", source) or not re.fullmatch(r"[a-z0-9_-]+", entity):
        raise ValueError("source/entity must contain lowercase letters, digits, underscores or hyphens")
    return f"s3a://{bucket}/bronze/{source}/{entity}/"


def safe_count(df) -> int:
    return int(df.count())
