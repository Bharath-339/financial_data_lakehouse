"""Normalize immutable Lambda Bronze envelopes into source-specific Silver Iceberg tables."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window

from lakehouse.quality_rules import rbi_field_candidates
from lakehouse.spark.common import (
    create_spark, ensure_database, merge_iceberg, parse_args, path_for_bronze,
    run_metadata, table_name, write_json_audit,
)


def _field_type(schema: T.StructType, path: str):
    current: Any = schema
    for part in path.split("."):
        if not isinstance(current, T.StructType):
            return None
        found = next((field for field in current.fields if field.name == part), None)
        if found is None:
            return None
        current = found.dataType
    return current


def _col_or_null(df: DataFrame, path: str, data_type: str = "string"):
    return F.col(path).cast(data_type) if _field_type(df.schema, path) is not None else F.lit(None).cast(data_type)


def _read_bronze(spark, bucket: str, source: str, entity: str,
                 extract_date: str | None = None, run_id: str | None = None) -> DataFrame:
    path = path_for_bronze(bucket, source, entity)
    df = (spark.read
          .option("recursiveFileLookup", "true")
          .option("pathGlobFilter", "*.json.gz")
          .option("mode", "FAILFAST")
          .json(path)
          .withColumn("bronze_source_file", F.input_file_name()))
    if extract_date:
        df = df.where(F.col("metadata.extract_date") == F.lit(extract_date))
    if run_id:
        df = df.where(F.col("metadata.run_id") == F.lit(run_id))
    return df


def _base_metadata(df: DataFrame) -> list:
    return [
        _col_or_null(df, "metadata.run_id").alias("ingestion_run_id"),
        F.to_timestamp(_col_or_null(df, "metadata.ingested_at")).alias("ingested_at"),
        F.to_date(_col_or_null(df, "metadata.extract_date")).alias("extract_date"),
        _col_or_null(df, "metadata.entity_key").alias("source_entity_key"),
        _col_or_null(df, "bronze_source_file").alias("bronze_source_file"),
    ]


def _deduplicate(df: DataFrame, keys: list[str]) -> DataFrame:
    order = [F.col("ingested_at").desc_nulls_last(), F.col("ingestion_run_id").desc_nulls_last(),
             F.col("bronze_source_file").desc_nulls_last()]
    window = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_dedupe_rank", F.row_number().over(window)).where(F.col("_dedupe_rank") == 1).drop("_dedupe_rank")


def _quarantine(df: DataFrame, *, bucket: str, source: str, run_id: str) -> int:
    count = df.count()
    if count:
        path = f"s3a://{bucket}/quarantine/{source}/run_id={run_id}/"
        (df.withColumn("quarantined_at", F.current_timestamp())
           .write.mode("append").format("parquet").save(path))
    return count


def _quality_reason(conditions: list[tuple[Any, str]]):
    reason = F.lit(None).cast("string")
    for condition, label in reversed(conditions):
        reason = F.when(condition, F.lit(label)).otherwise(reason)
    return reason


def transform_upstox(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    candles = raw.select(
        *_base_metadata(raw),
        F.col("metadata.entity_key").alias("instrument_key"),
        F.explode_outer(F.col("response.data.candles")).alias("candle"),
    )
    candle = F.col("candle")
    timestamp_text = candle.getItem(0).cast("string")
    normalized = candles.select(
        "instrument_key",
        F.to_timestamp(timestamp_text).alias("candle_timestamp"),
        # Upstox candle timestamps include +05:30. Extract the exchange-local date
        # from the source string to avoid a UTC date shift at midnight IST.
        F.to_date(F.substring(timestamp_text, 1, 10), "yyyy-MM-dd").alias("trading_date"),
        F.lit("1d").alias("interval"),
        candle.getItem(1).cast(T.DecimalType(20, 6)).alias("open_price"),
        candle.getItem(2).cast(T.DecimalType(20, 6)).alias("high_price"),
        candle.getItem(3).cast(T.DecimalType(20, 6)).alias("low_price"),
        candle.getItem(4).cast(T.DecimalType(20, 6)).alias("close_price"),
        candle.getItem(5).cast("long").alias("volume"),
        candle.getItem(6).cast("long").alias("open_interest"),
        F.lit("upstox").alias("source"), "source_entity_key", "ingestion_run_id", "ingested_at",
        "extract_date", "bronze_source_file",
    )
    invalid_reason = _quality_reason([
        (F.col("instrument_key").isNull() | (F.length(F.trim("instrument_key")) == 0), "instrument_key_missing"),
        (F.col("trading_date").isNull(), "trading_date_invalid"),
        (F.col("open_price").isNull() | F.col("high_price").isNull() |
         F.col("low_price").isNull() | F.col("close_price").isNull(), "ohlc_missing_or_invalid"),
        (F.col("volume").isNull() | (F.col("volume") < 0), "volume_missing_or_negative"),
        ((F.col("open_price") <= 0) | (F.col("high_price") <= 0) |
         (F.col("low_price") <= 0) | (F.col("close_price") <= 0), "ohlc_non_positive"),
        (F.col("high_price") < F.greatest("open_price", "close_price", "low_price"), "high_below_ohlc"),
        (F.col("low_price") > F.least("open_price", "close_price", "high_price"), "low_above_ohlc"),
    ])
    tagged = normalized.withColumn("dq_reason", invalid_reason)
    invalid = tagged.where(F.col("dq_reason").isNotNull())
    valid = tagged.where(F.col("dq_reason").isNull()).drop("dq_reason")
    valid = _deduplicate(valid, ["instrument_key", "trading_date", "interval", "source"])
    return valid, invalid



def transform_upstox_instrument_master(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Normalize the daily Upstox BOD instrument file while retaining dated versions."""
    response_type = _field_type(raw.schema, "response")
    if isinstance(response_type, T.ArrayType):
        records = raw.select(*_base_metadata(raw), F.explode_outer("response").alias("instrument"))
    else:
        array_path = _first_array_path(raw.schema, ["response.data", "response.instruments", "response.records"])
        records = raw.select(*_base_metadata(raw), F.explode_outer(F.col(array_path)).alias("instrument"))
    item = F.col("instrument")
    fields = {name.lower(): name for name in (_field_type(raw.schema, "response").elementType.fieldNames()
              if isinstance(response_type, T.ArrayType) and isinstance(response_type.elementType, T.StructType)
              else _field_type(raw.schema, _first_array_path(raw.schema, ["response.data", "response.instruments", "response.records"])).elementType.fieldNames())}

    def field(*aliases, data_type="string"):
        for alias in aliases:
            actual = fields.get(alias.lower())
            if actual:
                return item.getField(actual).cast(data_type)
        return F.lit(None).cast(data_type)

    instrument_key = field("instrument_key")
    master_date = F.to_date(F.col("extract_date"))
    record_json = F.to_json(item)
    normalized = records.select(
        instrument_key.alias("instrument_key"),
        master_date.alias("master_date"),
        F.upper(field("segment")).alias("segment"),
        field("trading_symbol").alias("trading_symbol"),
        field("name").alias("name"),
        field("short_name").alias("short_name"),
        F.upper(field("exchange")).alias("exchange"),
        field("isin").alias("isin"),
        field("instrument_type").alias("instrument_type"),
        field("security_type").alias("security_type"),
        field("exchange_token").alias("exchange_token"),
        field("lot_size", data_type="int").alias("lot_size"),
        field("tick_size", data_type="decimal(20,8)").alias("tick_size"),
        field("expiry").alias("expiry_raw"),
        field("strike_price", data_type="decimal(24,8)").alias("strike_price"),
        field("underlying_symbol").alias("underlying_symbol"),
        field("underlying_key").alias("underlying_key"),
        record_json.alias("source_record_json"),
        F.sha2(record_json, 256).alias("record_hash"),
        F.lit("upstox").alias("source"),
        "source_entity_key", "ingestion_run_id", "ingested_at", "extract_date", "bronze_source_file",
    )
    reason = _quality_reason([
        (F.col("instrument_key").isNull() | (F.length(F.trim("instrument_key")) == 0), "instrument_key_missing"),
        (F.col("segment").isNull() | (F.length(F.trim("segment")) == 0), "segment_missing"),
        (F.col("trading_symbol").isNull() | (F.length(F.trim("trading_symbol")) == 0), "trading_symbol_missing"),
    ])
    tagged = normalized.withColumn("dq_reason", reason)
    invalid = tagged.where(F.col("dq_reason").isNotNull())
    valid = tagged.where(F.col("dq_reason").isNull()).drop("dq_reason")
    valid = _deduplicate(valid, ["instrument_key", "master_date"])
    return valid, invalid

def transform_coingecko(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    exploded = raw.select(
        *_base_metadata(raw),
        F.explode_outer(F.col("response")).alias("coin"),
        _col_or_null(raw, "metadata.request.query.vs_currency").alias("requested_currency"),
    )
    coin = F.col("coin")
    requested_currency = F.coalesce(
        F.col("requested_currency"),
        F.regexp_extract(F.col("source_entity_key"), r"vs_currency=([A-Za-z]+)", 1),
    )
    snapshot_timestamp = F.coalesce(F.to_timestamp(coin.getField("last_updated")), F.col("ingested_at"))
    normalized = exploded.select(
        coin.getField("id").cast("string").alias("crypto_id"),
        F.lower(coin.getField("symbol").cast("string")).alias("symbol"),
        coin.getField("name").cast("string").alias("name"),
        F.lower(requested_currency).alias("vs_currency"),
        snapshot_timestamp.alias("snapshot_timestamp"),
        F.to_date(F.col("extract_date")).alias("snapshot_date"),
        coin.getField("current_price").cast(T.DecimalType(38, 12)).alias("current_price"),
        coin.getField("market_cap").cast(T.DecimalType(38, 2)).alias("market_cap"),
        coin.getField("total_volume").cast(T.DecimalType(38, 2)).alias("total_volume"),
        coin.getField("circulating_supply").cast(T.DecimalType(38, 12)).alias("circulating_supply"),
        coin.getField("total_supply").cast(T.DecimalType(38, 12)).alias("total_supply"),
        coin.getField("max_supply").cast(T.DecimalType(38, 12)).alias("max_supply"),
        coin.getField("market_cap_rank").cast("int").alias("market_cap_rank"),
        F.lit("coingecko").alias("source"), "source_entity_key", "ingestion_run_id", "ingested_at",
        "extract_date", "bronze_source_file",
    )
    reason = _quality_reason([
        (F.col("crypto_id").isNull() | (F.length(F.trim("crypto_id")) == 0), "crypto_id_missing"),
        (F.col("vs_currency").isNull() | (F.length(F.trim("vs_currency")) == 0), "vs_currency_missing"),
        (F.col("snapshot_date").isNull(), "snapshot_date_missing"),
        (F.col("current_price").isNull() | (F.col("current_price") < 0), "current_price_invalid"),
    ])
    tagged = normalized.withColumn("dq_reason", reason)
    invalid = tagged.where(F.col("dq_reason").isNotNull())
    valid = tagged.where(F.col("dq_reason").isNull()).drop("dq_reason")
    valid = _deduplicate(valid, ["crypto_id", "vs_currency", "snapshot_date"])
    return valid, invalid


def transform_frankfurter(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    exploded = raw.select(*_base_metadata(raw), F.explode_outer(F.col("response")).alias("rate"))
    rate = F.col("rate")
    normalized = exploded.select(
        F.to_date(rate.getField("date")).alias("rate_date"),
        F.upper(rate.getField("base").cast("string")).alias("base_currency"),
        F.upper(rate.getField("quote").cast("string")).alias("quote_currency"),
        rate.getField("rate").cast(T.DecimalType(24, 12)).alias("exchange_rate"),
        F.lit("frankfurter").alias("source"), "source_entity_key", "ingestion_run_id", "ingested_at",
        "extract_date", "bronze_source_file",
    )
    reason = _quality_reason([
        (F.col("rate_date").isNull(), "rate_date_missing"),
        (F.col("base_currency").isNull() | F.col("quote_currency").isNull(), "currency_missing"),
        (F.col("exchange_rate").isNull() | (F.col("exchange_rate") <= 0), "exchange_rate_invalid"),
    ])
    tagged = normalized.withColumn("dq_reason", reason)
    invalid = tagged.where(F.col("dq_reason").isNotNull())
    valid = tagged.where(F.col("dq_reason").isNull()).drop("dq_reason")
    valid = _deduplicate(valid, ["rate_date", "base_currency", "quote_currency", "source"])
    return valid, invalid


def _first_array_path(schema: T.StructType, candidates: list[str]) -> str:
    for path in candidates:
        dtype = _field_type(schema, path)
        if isinstance(dtype, T.ArrayType):
            return path
    raise ValueError("RBI response has no supported array of rows; inspect table metadata and add a mapping")


def _row_field_names(row_type: T.DataType) -> dict[str, str]:
    if isinstance(row_type, T.StructType):
        return {field.name.lower(): field.name for field in row_type.fields}
    return {}


def _pick_row_field(row_col, names: dict[str, str], aliases: tuple[str, ...]):
    for alias in aliases:
        actual = names.get(alias.lower())
        if actual:
            return row_col.getField(actual).cast("string")
    return F.lit(None).cast("string")


def _parse_period(raw_col):
    return F.coalesce(
        F.to_date(raw_col),
        F.to_date(raw_col, "dd-MM-yyyy"),
        F.to_date(raw_col, "dd/MM/yyyy"),
        F.to_date(raw_col, "MMM-yyyy"),
        F.to_date(raw_col, "MMMM-yyyy"),
        F.to_date(raw_col, "yyyy"),
    )


def transform_rbi(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    array_path = _first_array_path(raw.schema, [
        "response.rows", "response.data.rows", "response.results", "response.records",
        "response.items", "response.data", "response.results.rows", "response",
    ])
    row_type = _field_type(raw.schema, array_path).elementType
    names = _row_field_names(row_type)
    aliases = rbi_field_candidates()
    exploded = raw.select(
        *_base_metadata(raw),
        F.col("metadata.entity_key").alias("table_key"),
        F.explode_outer(F.col(array_path)).alias("r"),
    )
    row_col = F.col("r")
    period_raw = _pick_row_field(row_col, names, aliases["period"])
    indicator = _pick_row_field(row_col, names, aliases["indicator"])
    value_raw = _pick_row_field(row_col, names, aliases["value"])
    unit = _pick_row_field(row_col, names, aliases["unit"])
    available_raw = _pick_row_field(row_col, names, aliases["available_date"])
    row_json = F.to_json(row_col)
    normalized = exploded.select(
        F.split(F.col("table_key"), r"\.").getItem(0).alias("rbi_schema"),
        F.split(F.col("table_key"), r"\.").getItem(1).alias("rbi_table"),
        _parse_period(period_raw).alias("observation_period"),
        period_raw.alias("observation_period_raw"),
        F.coalesce(indicator, F.col("table_key")).alias("indicator_code"),
        value_raw.alias("value_raw"),
        F.regexp_replace(value_raw, ",", "").cast(T.DecimalType(30, 8)).alias("observation_value"),
        unit.alias("unit"),
        _parse_period(available_raw).alias("available_date"),
        F.to_timestamp("ingested_at").alias("data_vintage"),
        row_json.alias("source_row_json"),
        F.sha2(row_json, 256).alias("row_hash"),
        F.lit("rbi").alias("source"), "source_entity_key", "ingestion_run_id", "ingested_at",
        "extract_date", "bronze_source_file",
    )
    review_reason = _quality_reason([
        (F.col("observation_period").isNull(), "observation_period_needs_table_mapping"),
        (F.col("observation_value").isNull(), "numeric_value_needs_table_mapping"),
    ])
    tagged = (normalized
        .withColumn("dq_reason", review_reason)
        .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("REVIEW")))
    # RBI is intentionally non-destructive: rows that need table-specific mapping are
    # retained in Silver with REVIEW status and copied to quarantine for inspection.
    review = tagged.where(F.col("dq_status") == "REVIEW")
    silver = _deduplicate(tagged, ["rbi_schema", "rbi_table", "row_hash"])
    return silver, review


def _rbi_row_frame(raw: DataFrame, array_candidates: list[str]) -> tuple[DataFrame, T.DataType]:
    """Return one row per RBI catalogue item; tolerate object-shaped singletons."""
    try:
        array_path = _first_array_path(raw.schema, array_candidates)
        row_type = _field_type(raw.schema, array_path).elementType
        rows = raw.select(*_base_metadata(raw), F.explode_outer(F.col(array_path)).alias("r"))
        return rows, row_type
    except ValueError:
        response_type = _field_type(raw.schema, "response")
        if response_type is None:
            raise
        rows = raw.select(*_base_metadata(raw), F.col("response").alias("r"))
        return rows, response_type


def transform_rbi_catalogue(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Persist discovered DBIE catalogue entries without guessing their semantics."""
    rows, row_type = _rbi_row_frame(raw, [
        "response.tables", "response.data.tables", "response.results",
        "response.records", "response.items", "response.data", "response",
    ])
    names = _row_field_names(row_type)
    row_col = F.col("r")
    schema_value = _pick_row_field(row_col, names, (
        "schema", "schema_name", "database", "database_name", "table_schema",
    ))
    table_value = _pick_row_field(row_col, names, (
        "table", "table_name", "name", "table_id", "id", "dataset_id",
    ))
    title_value = _pick_row_field(row_col, names, (
        "title", "display_name", "table_title", "name", "label",
    ))
    description_value = _pick_row_field(row_col, names, (
        "description", "table_description", "definition", "notes",
    ))
    row_json = F.to_json(row_col)
    normalized = rows.select(
        F.coalesce(schema_value, F.split(F.col("source_entity_key"), r"\.").getItem(0)).alias("rbi_schema"),
        F.coalesce(table_value, F.split(F.col("source_entity_key"), r"\.").getItem(1)).alias("rbi_table"),
        title_value.alias("table_title"),
        description_value.alias("table_description"),
        row_json.alias("catalogue_json"),
        F.sha2(row_json, 256).alias("row_hash"),
        F.lit("rbi").alias("source"), "source_entity_key", "ingestion_run_id", "ingested_at",
        "extract_date", "bronze_source_file",
    )
    reason = _quality_reason([
        (F.col("rbi_schema").isNull() | (F.length(F.trim("rbi_schema")) == 0), "catalogue_schema_identifier_missing"),
        (F.col("rbi_table").isNull() | (F.length(F.trim("rbi_table")) == 0), "catalogue_table_identifier_missing"),
    ])
    tagged = (normalized.withColumn("dq_reason", reason)
              .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("REVIEW")))
    review = tagged.where(F.col("dq_status") == "REVIEW")
    valid_and_review = _deduplicate(tagged, ["row_hash", "source_entity_key"])
    return valid_and_review, review


def transform_rbi_metadata(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Store the complete metadata document and a stable hash for every table vintage."""
    entity_key = _col_or_null(raw, "metadata.entity_key")
    response_json = F.to_json(F.col("response"))
    normalized = raw.select(
        *_base_metadata(raw),
        F.split(entity_key, r"\.").getItem(0).alias("rbi_schema"),
        F.split(entity_key, r"\.").getItem(1).alias("rbi_table"),
        response_json.alias("metadata_json"),
        F.sha2(response_json, 256).alias("metadata_hash"),
        F.lit("rbi").alias("source"),
    )
    reason = _quality_reason([
        (F.col("rbi_schema").isNull() | (F.length(F.trim("rbi_schema")) == 0), "metadata_schema_missing"),
        (F.col("rbi_table").isNull() | (F.length(F.trim("rbi_table")) == 0), "metadata_table_missing"),
    ])
    tagged = (normalized.withColumn("dq_reason", reason)
              .withColumn("dq_status", F.when(F.col("dq_reason").isNull(), "VALID").otherwise("REVIEW")))
    review = tagged.where(F.col("dq_status") == "REVIEW")
    return _deduplicate(tagged, ["rbi_schema", "rbi_table", "metadata_hash"]), review


TRANSFORMERS = {
    ("upstox", "equity_ohlcv"): transform_upstox,
    ("upstox", "instrument_master"): transform_upstox_instrument_master,
    ("coingecko", "market_snapshot"): transform_coingecko,
    ("frankfurter", "daily_rates"): transform_frankfurter,
    ("rbi", "table_rows"): transform_rbi,
    ("rbi", "table_catalogue"): transform_rbi_catalogue,
    ("rbi", "table_metadata"): transform_rbi_metadata,
}

TABLE_CONFIG = {
    ("upstox", "equity_ohlcv"): ("equity_daily_ohlcv", ["instrument_key", "trading_date", "interval", "source"], "months(trading_date)"),
    ("upstox", "instrument_master"): ("upstox_instrument_master", ["instrument_key", "master_date"], "months(master_date)"),
    ("coingecko", "market_snapshot"): ("crypto_market_snapshots", ["crypto_id", "vs_currency", "snapshot_date"], "months(snapshot_date)"),
    ("frankfurter", "daily_rates"): ("fx_daily_rates", ["rate_date", "base_currency", "quote_currency", "source"], "months(rate_date)"),
    ("rbi", "table_rows"): ("macro_observations", ["rbi_schema", "rbi_table", "row_hash"], "months(extract_date)"),
    ("rbi", "table_catalogue"): ("rbi_table_catalogue", ["source_entity_key", "row_hash"], "months(extract_date)"),
    ("rbi", "table_metadata"): ("rbi_table_metadata", ["rbi_schema", "rbi_table", "metadata_hash"], "months(extract_date)"),
}


def process(args) -> dict[str, Any]:
    if not args.source or not args.entity:
        raise ValueError("--source and --entity are required")
    key = (args.source, args.entity)
    if key not in TRANSFORMERS:
        raise ValueError(f"Unsupported Bronze entity: {args.source}/{args.entity}")

    spark = create_spark("financial-bronze-to-silver", args)
    started = run_metadata(args, "bronze_to_silver")
    run_id = started["job_run_id"]
    try:
        # Validate the exact extraction slice before Spark parses any provider payload.
        # This prevents silently transforming a malformed or cross-routed Bronze object.
        import boto3
        from botocore.config import Config
        from lakehouse.bronze_validation import validate_prefix
        endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("FLOCI_ENDPOINT_URL")
        region = os.environ.get("AWS_DEFAULT_REGION", os.environ.get("AWS_REGION", "us-east-1"))
        s3_config = Config(s3={"addressing_style": "path"}) if endpoint else None
        s3_client = boto3.client("s3", region_name=region, endpoint_url=endpoint, config=s3_config)
        bronze_validation = validate_prefix(
            s3=s3_client, bucket=args.bucket, source=args.source, entity=args.entity,
            run_date=args.extract_date, run_id_filter=args.run_id, fail_on_invalid=True,
        )
        raw = _read_bronze(spark, args.bucket, args.source, args.entity, args.extract_date, args.run_id)
        input_count = raw.count()
        if input_count == 0:
            raise ValueError(
                f"No Bronze objects found for source={args.source}, entity={args.entity}, "
                f"extract_date={args.extract_date}, run_id={args.run_id}"
            )

        silver, quarantine = TRANSFORMERS[key](raw)
        silver_count = silver.count()
        quarantined_count = _quarantine(quarantine, bucket=args.bucket, source=args.source, run_id=run_id)

        ensure_database(spark, args.catalog, args.silver_database)
        table, merge_keys, partition = TABLE_CONFIG[key]
        if silver_count:
            merge_iceberg(
                spark, silver,
                target=table_name(args.catalog, args.silver_database, table),
                keys=merge_keys,
                partition_transform=partition,
            )

        report = {
            **started,
            "status": "SUCCESS",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "source": args.source,
            "entity": args.entity,
            "extract_date_filter": args.extract_date,
            "run_id_filter": args.run_id,
            "input_envelope_count": input_count,
            "bronze_validation": bronze_validation,
            "silver_row_count": silver_count,
            "quarantined_row_count": quarantined_count,
            "silver_table": f"{args.catalog}.{args.silver_database}.{table}",
        }
        audit_key = (
            f"audit/processing_runs/layer=silver/run_date={datetime.now(timezone.utc).date().isoformat()}"
            f"/{run_id}.json"
        )
        write_json_audit(args.bucket, audit_key, report)
        report["audit_key"] = audit_key
        return report
    except Exception as exc:
        report = {
            **started,
            "status": "FAILED",
            "source": args.source,
            "entity": args.entity,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "error": f"{type(exc).__name__}: {exc}"[:1500],
        }
        try:
            write_json_audit(
                args.bucket,
                f"audit/processing_runs/layer=silver/run_date={datetime.now(timezone.utc).date().isoformat()}/{run_id}.json",
                report,
            )
        except Exception:
            pass
        raise
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    print(json.dumps(process(args), indent=2, default=str))


if __name__ == "__main__":
    main()
