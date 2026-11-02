"""Build SCD2 dimensions, atomic facts and analytical marts from Silver Iceberg tables.

The Gold job intentionally reads the complete Silver history before calculating
window metrics. It is designed for a personal-sized daily market dataset; if data
volume grows, replace the full-history calculations with affected-partition
recomputation while preserving the same table contracts.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.window import Window

from lakehouse.spark.common import (
    create_spark, ensure_database, merge_iceberg, parse_args, run_metadata,
    table_name, write_json_audit, _ensure_additive_schema,
)

SOURCE_IDS = {"upstox": 1, "coingecko": 2, "frankfurter": 3, "rbi": 4}


def _exists(spark, catalog: str, database: str, table: str) -> bool:
    try:
        return spark.catalog.tableExists(f"{catalog}.{database}.{table}")
    except Exception:
        try:
            spark.table(table_name(catalog, database, table)).limit(1).count()
            return True
        except Exception:
            return False


def _read_if_exists(spark, catalog: str, database: str, table: str):
    return spark.table(table_name(catalog, database, table)) if _exists(spark, catalog, database, table) else None


def _load_reference(spark, reference_dir: str, filename: str, schema: T.StructType) -> DataFrame:
    """Load a reference CSV. A missing file is a warning, a corrupt file is an error."""
    path = f"{reference_dir.rstrip('/')}/{filename}"
    try:
        return spark.read.option("header", "true").schema(schema).csv(path)
    except Exception as exc:
        # S3/Floci NoSuchKey and local absent files are tolerated for optional reference data.
        message = str(exc).lower()
        if any(token in message for token in ("nosuchkey", "path does not exist", "filenotfound", "no such file")):
            print(f"WARNING: optional reference file not found: {path}")
            return spark.createDataFrame([], schema)
        raise


def _merge_scd2(spark, staged: DataFrame, target: str, business_key: str) -> None:
    """Close changed current rows and insert exactly one new current version.

    Stage must have business key, attribute_hash, effective_from/effective_to,
    is_current and a deterministic surrogate key. Re-running the same stage is
    idempotent. At date grain, multiple changes on the same day collapse to the
    latest observed attributes because a date interval cannot represent intraday versions.
    """
    if staged.limit(1).count() == 0:
        return
    staged = staged.dropDuplicates([business_key])
    view = f"_scd2_{business_key}"
    staged.createOrReplaceTempView(view)
    try:
        spark.sql(f"""CREATE TABLE IF NOT EXISTS {target} USING iceberg
            AS SELECT * FROM `{view}` WHERE 1 = 0""")
        _ensure_additive_schema(spark, staged, target)
        # Date-grain SCD2 cannot represent two attribute versions on the same day.
        # Close an older version only when the new effective date is later. If a
        # dimension changes again on the same day, update that day's current row.
        spark.sql(f"""MERGE INTO {target} t USING `{view}` s
            ON t.`{business_key}` = s.`{business_key}` AND t.is_current = true
            WHEN MATCHED AND t.attribute_hash <> s.attribute_hash
                 AND t.effective_from < s.effective_from
            THEN UPDATE SET t.is_current = false,
                            t.effective_to = date_sub(s.effective_from, 1)""")
        columns = staged.columns
        insert_columns = ", ".join(f"`{column}`" for column in columns)
        insert_values = ", ".join(f"s.`{column}`" for column in columns)
        update_columns = [column for column in columns if column != business_key]
        update_set = ", ".join(f"t.`{column}` = s.`{column}`" for column in update_columns)
        matched = (f"WHEN MATCHED AND t.attribute_hash <> s.attribute_hash THEN UPDATE SET {update_set} "
                   if update_set else "")
        spark.sql(f"""MERGE INTO {target} t USING `{view}` s
            ON t.`{business_key}` = s.`{business_key}` AND t.is_current = true
            {matched}
            WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})""")
    finally:
        spark.catalog.dropTempView(view)


def _add_scd2_columns(base: DataFrame, business_key: str, attributes: list[str],
                      first_seen_col: str, existing: DataFrame | None, sk_name: str) -> DataFrame:
    """Prepare an SCD2 stage, preserving original effective date for unchanged rows."""
    staged = base.withColumn(
        "attribute_hash",
        F.sha2(F.concat_ws("||", *[
            F.coalesce(F.col(column).cast("string"), F.lit("<NULL>")) for column in attributes
        ]), 256),
    )
    if existing is not None:
        current = (existing.where(F.col("is_current") == F.lit(True))
                   .select(business_key, F.col("attribute_hash").alias("_old_hash"),
                           F.col("effective_from").alias("_old_effective_from")))
        staged = staged.join(current, business_key, "left")
        changed_effective = (F.col("change_effective_date")
                             if "change_effective_date" in staged.columns else F.current_date())
        effective = (F.when(F.col("_old_hash").isNull(), F.col(first_seen_col))
                     .when(F.col("_old_hash") == F.col("attribute_hash"), F.col("_old_effective_from"))
                     .otherwise(F.coalesce(changed_effective, F.current_date())))
        staged = staged.withColumn("effective_from", effective).drop("_old_hash", "_old_effective_from")
    else:
        staged = staged.withColumn("effective_from", F.col(first_seen_col))
    staged = (staged.withColumn("effective_to", F.lit(None).cast("date"))
              .withColumn("is_current", F.lit(True))
              .withColumn(sk_name, F.sha2(F.concat_ws("||", F.col(business_key).cast("string"),
                                                        F.col("effective_from").cast("string"),
                                                        F.col("attribute_hash")), 256)))
    drop_columns = [first_seen_col]
    if "change_effective_date" in staged.columns:
        drop_columns.append("change_effective_date")
    return staged.drop(*drop_columns)


def _date_key(column: str):
    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


def _merge_if_data(spark, df: DataFrame | None, target: str, keys: list[str],
                    partition: str | None = None) -> int:
    if df is None:
        return 0
    count = df.count()
    if not count:
        return 0
    null_key = F.lit(False)
    for key in keys:
        null_key = null_key | F.col(key).isNull()
    if df.where(null_key).limit(1).count():
        raise ValueError(f"Gold data-quality failure for {target}: null business-key value in {keys}")
    duplicate = (df.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count())
    if duplicate:
        raise ValueError(f"Gold data-quality failure for {target}: duplicate business key {keys}")
    merge_iceberg(spark, df, target=target, keys=keys, partition_transform=partition)
    return count


def _build_instrument_dimension(spark, args, equity: DataFrame | None, master: DataFrame | None) -> int:
    if equity is None:
        return 0
    key_schema = T.StructType([
        T.StructField("instrument_key", T.StringType()), T.StructField("symbol", T.StringType()),
        T.StructField("company_name", T.StringType()), T.StructField("exchange", T.StringType()),
        T.StructField("isin", T.StringType()), T.StructField("sector", T.StringType()),
        T.StructField("asset_type", T.StringType()),
    ])
    ref = _load_reference(spark, args.reference_dir, "instruments.csv", key_schema)
    seen = equity.groupBy("instrument_key").agg(F.min("trading_date").alias("first_seen_date"))
    master_latest = None
    if master is not None:
        master_window = Window.partitionBy("instrument_key").orderBy(F.col("master_date").desc(), F.col("ingested_at").desc())
        master_latest = (master.withColumn("_rn", F.row_number().over(master_window))
            .where(F.col("_rn") == 1).drop("_rn")
            .select("instrument_key", "segment", "trading_symbol", "name", "short_name",
                    F.col("exchange").alias("master_exchange"), F.col("isin").alias("master_isin"),
                    "instrument_type", "security_type", F.col("master_date").alias("change_effective_date")))
    joined = seen
    if master_latest is not None:
        joined = joined.join(master_latest, "instrument_key", "left")
    else:
        joined = (joined.withColumn("segment", F.lit(None).cast("string"))
                  .withColumn("trading_symbol", F.lit(None).cast("string"))
                  .withColumn("name", F.lit(None).cast("string"))
                  .withColumn("short_name", F.lit(None).cast("string"))
                  .withColumn("master_exchange", F.lit(None).cast("string"))
                  .withColumn("master_isin", F.lit(None).cast("string"))
                  .withColumn("instrument_type", F.lit(None).cast("string"))
                  .withColumn("security_type", F.lit(None).cast("string"))
                  .withColumn("change_effective_date", F.lit(None).cast("date")))
    # Reviewed reference fields override provider metadata; fields absent from the
    # provider (for example sector) can still be supplied in instruments.csv.
    base = (joined.join(ref, "instrument_key", "left")
        .select("instrument_key", "first_seen_date",
                F.coalesce("trading_symbol", "symbol", "instrument_key").alias("symbol"),
                F.coalesce("name", "short_name", "company_name", "trading_symbol", "instrument_key").alias("company_name"),
                F.coalesce(F.col("master_exchange"), F.col("exchange"), F.lit("UNKNOWN")).alias("exchange"),
                F.coalesce(F.col("master_isin"), F.col("isin")).alias("isin"), "sector",
                F.coalesce("asset_type",
                    F.when(F.col("segment").endswith("INDEX"), F.lit("INDEX"))
                     .when(F.col("segment").endswith("_EQ"), F.lit("EQUITY"))
                     .when(F.col("instrument_type").isNotNull(), F.upper(F.col("instrument_type")))
                     .otherwise(F.lit("EQUITY"))).alias("asset_type"),
                "segment", "instrument_type", "security_type", "change_effective_date"))
    target = table_name(args.catalog, args.gold_database, "dim_instrument")
    existing = _read_if_exists(spark, args.catalog, args.gold_database, "dim_instrument")
    staged = _add_scd2_columns(base, "instrument_key",
        ["symbol", "company_name", "exchange", "isin", "sector", "asset_type",
         "segment", "instrument_type", "security_type"],
        "first_seen_date", existing, "instrument_sk")
    _merge_scd2(spark, staged, target, "instrument_key")
    return staged.count()


def _build_crypto_dimension(spark, args, crypto: DataFrame | None) -> int:
    if crypto is None:
        return 0
    base = (crypto.groupBy("crypto_id").agg(
        F.max_by("name", "snapshot_timestamp").alias("name"),
        F.max_by("symbol", "snapshot_timestamp").alias("symbol"),
        F.min("snapshot_date").alias("first_seen_date")))
    target = table_name(args.catalog, args.gold_database, "dim_crypto_asset")
    existing = _read_if_exists(spark, args.catalog, args.gold_database, "dim_crypto_asset")
    staged = _add_scd2_columns(base, "crypto_id", ["name", "symbol"],
                               "first_seen_date", existing, "crypto_asset_sk")
    _merge_scd2(spark, staged, target, "crypto_id")
    return staged.count()


def build_dimensions(spark, args) -> dict[str, int]:
    catalog, silver_db, gold_db = args.catalog, args.silver_database, args.gold_database
    ensure_database(spark, catalog, gold_db)
    equity = _read_if_exists(spark, catalog, silver_db, "equity_daily_ohlcv")
    instrument_master = _read_if_exists(spark, catalog, silver_db, "upstox_instrument_master")
    crypto = _read_if_exists(spark, catalog, silver_db, "crypto_market_snapshots")
    fx = _read_if_exists(spark, catalog, silver_db, "fx_daily_rates")
    macro = _read_if_exists(spark, catalog, silver_db, "macro_observations")
    counts: dict[str, int] = {}

    counts["dim_instrument"] = _build_instrument_dimension(spark, args, equity, instrument_master)
    counts["dim_crypto_asset"] = _build_crypto_dimension(spark, args, crypto)

    # Currency reference includes provider currencies and explicitly declared currencies.
    currency_schema = T.StructType([
        T.StructField("currency_code", T.StringType()), T.StructField("currency_name", T.StringType())
    ])
    currency_parts = [_load_reference(spark, args.reference_dir, "currencies.csv", currency_schema)]
    if fx is not None:
        currency_parts += [fx.select(F.col("base_currency").alias("currency_code")),
                           fx.select(F.col("quote_currency").alias("currency_code"))]
    if crypto is not None:
        currency_parts.append(crypto.select(F.upper("vs_currency").alias("currency_code")))
    currencies = currency_parts[0]
    for part in currency_parts[1:]:
        currencies = currencies.unionByName(part, allowMissingColumns=True)
    currencies = (currencies.where(F.col("currency_code").isNotNull())
        .groupBy("currency_code").agg(F.first("currency_name", ignorenulls=True).alias("currency_name"))
        .withColumn("currency_name", F.coalesce("currency_name", "currency_code"))
        .withColumn("currency_id", F.col("currency_code")))
    counts["dim_currency"] = _merge_if_data(
        spark, currencies, table_name(catalog, gold_db, "dim_currency"), ["currency_id"])

    # RBI mappings are user-reviewed. The raw code remains available even if not mapped.
    macro_schema = T.StructType([
        T.StructField("rbi_schema", T.StringType()), T.StructField("rbi_table", T.StringType()),
        T.StructField("indicator_code", T.StringType()), T.StructField("indicator_name", T.StringType()),
        T.StructField("unit", T.StringType()),
    ])
    macro_ref = _load_reference(spark, args.reference_dir, "macro_indicators.csv", macro_schema)
    if macro is not None:
        observed = (macro.where(F.col("dq_status") == "VALID")
            .groupBy("rbi_schema", "rbi_table", "indicator_code")
            .agg(F.max("unit").alias("observed_unit"),
                 F.max("observation_period").alias("latest_observation_period")))
        indicators = (observed.join(macro_ref, ["rbi_schema", "rbi_table", "indicator_code"], "left")
            .select("rbi_schema", "rbi_table", "indicator_code",
                    F.coalesce("indicator_name", "indicator_code").alias("indicator_name"),
                    F.coalesce("unit", "observed_unit").alias("unit"), "latest_observation_period")
            .withColumn("indicator_id", F.sha2(F.concat_ws("||", "rbi_schema", "rbi_table", "indicator_code"), 256)))
        counts["dim_macro_indicator"] = _merge_if_data(
            spark, indicators, table_name(catalog, gold_db, "dim_macro_indicator"), ["indicator_id"])
    else:
        counts["dim_macro_indicator"] = 0

    # Date dimension is continuous so joins do not depend on weekends being observed.
    date_parts = []
    for frame, column in ((equity, "trading_date"), (crypto, "snapshot_date"),
                          (fx, "rate_date"), (macro, "observation_period")):
        if frame is not None:
            date_parts.append(frame.select(F.col(column).alias("calendar_date")))
    if date_parts:
        dates = date_parts[0]
        for part in date_parts[1:]:
            dates = dates.unionByName(part)
        bounds = dates.where(F.col("calendar_date").isNotNull()).agg(
            F.min("calendar_date").alias("min_date"), F.max("calendar_date").alias("max_date")).first()
        if bounds and bounds["min_date"] and bounds["max_date"]:
            dates = spark.sql(
                f"SELECT explode(sequence(date'{bounds['min_date']}', date'{bounds['max_date']}', interval 1 day)) AS calendar_date")
            dates = (dates.withColumn("date_key", _date_key("calendar_date"))
                .withColumn("year", F.year("calendar_date"))
                .withColumn("quarter", F.quarter("calendar_date"))
                .withColumn("month", F.month("calendar_date"))
                .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
                .withColumn("day", F.dayofmonth("calendar_date"))
                .withColumn("day_of_week", F.dayofweek("calendar_date"))
                .withColumn("day_name", F.date_format("calendar_date", "EEEE"))
                .withColumn("is_weekend", F.dayofweek("calendar_date").isin([1, 7])))
            counts["dim_date"] = _merge_if_data(
                spark, dates, table_name(catalog, gold_db, "dim_date"), ["date_key"])
    else:
        counts["dim_date"] = 0

    sources = spark.createDataFrame([
        (1, "upstox", "Indian equity market data"),
        (2, "coingecko", "Cryptocurrency market data"),
        (3, "frankfurter", "Foreign exchange rates"),
        (4, "rbi", "Indian macroeconomic observations"),
    ], ["source_id", "source_name", "description"])
    counts["dim_data_source"] = _merge_if_data(
        spark, sources, table_name(catalog, gold_db, "dim_data_source"), ["source_id"])
    return counts


def _join_scd2(fact: DataFrame, dim: DataFrame | None, business_key: str,
               date_column: str, surrogate_key: str) -> DataFrame:
    if dim is None:
        return fact.withColumn(surrogate_key, F.lit(None).cast("string"))
    f, d = fact.alias("f"), dim.alias("d")
    condition = ((F.col(f"f.{business_key}") == F.col(f"d.{business_key}")) &
                 (F.col(f"f.{date_column}") >= F.col("d.effective_from")) &
                 (F.col(f"d.effective_to").isNull() | (F.col(f"f.{date_column}") <= F.col("d.effective_to"))))
    return f.join(d, condition, "left").select("f.*", F.col(f"d.{surrogate_key}").alias(surrogate_key))


def _join_date(fact: DataFrame, date_column: str) -> DataFrame:
    return fact.withColumn("date_key", _date_key(date_column))


def build_equity_gold(spark, args) -> DataFrame | None:
    silver = _read_if_exists(spark, args.catalog, args.silver_database, "equity_daily_ohlcv")
    if silver is None:
        return None
    w = Window.partitionBy("instrument_key").orderBy("trading_date")
    w7, w20, w30 = w.rowsBetween(-6, 0), w.rowsBetween(-19, 0), w.rowsBetween(-29, 0)
    df = (silver.withColumn("previous_close", F.lag("close_price").over(w))
        .withColumn("daily_return", F.when(F.col("previous_close") > 0,
            F.col("close_price").cast("double") / F.col("previous_close").cast("double") - 1.0))
        .withColumn("sma_7", F.avg("close_price").over(w7))
        .withColumn("sma_20", F.avg("close_price").over(w20))
        .withColumn("close_30_observations_ago", F.first_value("close_price").over(w30))
        .withColumn("return_30_observations", F.when(F.col("close_30_observations_ago") > 0,
            F.col("close_price").cast("double") / F.col("close_30_observations_ago").cast("double") - 1.0))
        .withColumn("volatility_20", F.stddev_samp("daily_return").over(w20))
        .withColumn("source_id", F.lit(SOURCE_IDS["upstox"]))
        .transform(lambda d: _join_date(d, "trading_date")))
    dim = _read_if_exists(spark, args.catalog, args.gold_database, "dim_instrument")
    return _join_scd2(df, dim, "instrument_key", "trading_date", "instrument_sk")


def build_crypto_gold(spark, args) -> DataFrame | None:
    silver = _read_if_exists(spark, args.catalog, args.silver_database, "crypto_market_snapshots")
    if silver is None:
        return None
    latest = Window.partitionBy("crypto_id", "vs_currency", "snapshot_date").orderBy(F.col("snapshot_timestamp").desc())
    df = silver.withColumn("_rn", F.row_number().over(latest)).where(F.col("_rn") == 1).drop("_rn")
    w = Window.partitionBy("crypto_id", "vs_currency").orderBy("snapshot_date")
    w7, w30 = w.rowsBetween(-6, 0), w.rowsBetween(-29, 0)
    df = (df.withColumn("previous_price", F.lag("current_price").over(w))
        .withColumn("daily_return", F.when(F.col("previous_price") > 0,
            F.col("current_price").cast("double") / F.col("previous_price").cast("double") - 1.0))
        .withColumn("price_sma_7", F.avg("current_price").over(w7))
        .withColumn("price_sma_30", F.avg("current_price").over(w30))
        .withColumn("price_30_observations_ago", F.first_value("current_price").over(w30))
        .withColumn("return_30_observations", F.when(F.col("price_30_observations_ago") > 0,
            F.col("current_price").cast("double") / F.col("price_30_observations_ago").cast("double") - 1.0))
        .withColumn("volatility_30", F.stddev_samp("daily_return").over(w30))
        .withColumn("source_id", F.lit(SOURCE_IDS["coingecko"]))
        .withColumn("currency_id", F.upper("vs_currency"))
        .transform(lambda d: _join_date(d, "snapshot_date")))
    dim = _read_if_exists(spark, args.catalog, args.gold_database, "dim_crypto_asset")
    return _join_scd2(df, dim, "crypto_id", "snapshot_date", "crypto_asset_sk")


def build_fx_gold(spark, args) -> DataFrame | None:
    silver = _read_if_exists(spark, args.catalog, args.silver_database, "fx_daily_rates")
    if silver is None:
        return None
    w = Window.partitionBy("base_currency", "quote_currency").orderBy("rate_date")
    w30 = w.rowsBetween(-29, 0)
    df = (silver.withColumn("previous_rate", F.lag("exchange_rate").over(w))
        .withColumn("daily_change_pct", F.when(F.col("previous_rate") > 0,
            F.col("exchange_rate").cast("double") / F.col("previous_rate").cast("double") - 1.0))
        .withColumn("rate_sma_30", F.avg("exchange_rate").over(w30))
        .withColumn("source_id", F.lit(SOURCE_IDS["frankfurter"]))
        .transform(lambda d: _join_date(d, "rate_date")))
    return df.withColumn("base_currency_id", F.col("base_currency")).withColumn("quote_currency_id", F.col("quote_currency"))


def build_macro_gold(spark, args) -> DataFrame | None:
    silver = _read_if_exists(spark, args.catalog, args.silver_database, "macro_observations")
    if silver is None:
        return None
    valid = silver.where(F.col("dq_status") == "VALID")
    dim = _read_if_exists(spark, args.catalog, args.gold_database, "dim_macro_indicator")
    if dim is not None:
        mapped = dim.select(
            "rbi_schema", "rbi_table", "indicator_code", "indicator_id",
            "indicator_name", F.col("unit").alias("mapped_unit"),
        )
        valid = (valid.join(mapped, ["rbi_schema", "rbi_table", "indicator_code"], "left")
            .withColumn("indicator_name", F.coalesce(F.col("indicator_name"), F.col("indicator_code")))
            .withColumn("unit", F.coalesce(F.col("unit"), F.col("mapped_unit")))
            .drop("mapped_unit"))
    else:
        valid = (valid.withColumn("indicator_id", F.lit(None).cast("string"))
                 .withColumn("indicator_name", F.col("indicator_code")))
    return (valid.withColumn("data_vintage", F.coalesce(F.col("data_vintage"), F.col("ingested_at")))
        .withColumn("source_id", F.lit(SOURCE_IDS["rbi"]))
        .withColumn("date_key", _date_key("observation_period")))


def build_marts(spark, args, equity: DataFrame | None, crypto: DataFrame | None,
                fx: DataFrame | None, macro: DataFrame | None) -> dict[str, int]:
    catalog, gold_db = args.catalog, args.gold_database
    counts: dict[str, int] = {}
    overview_parts: list[DataFrame] = []

    if equity is not None:
        counts["fact_equity_daily_ohlcv"] = _merge_if_data(
            spark, equity, table_name(catalog, gold_db, "fact_equity_daily_ohlcv"),
            ["instrument_key", "trading_date", "interval", "source"], "months(trading_date)")
        # Preserve full metric history for reproducible historical analysis.
        counts["mart_equity_returns"] = _merge_if_data(
            spark, equity, table_name(catalog, gold_db, "mart_equity_returns"),
            ["instrument_key", "trading_date", "interval", "source"], "months(trading_date)")
        overview_parts.append(equity.groupBy(F.col("trading_date").alias("as_of_date")).agg(
            F.countDistinct("instrument_key").alias("asset_count"), F.avg("daily_return").alias("average_return"),
            F.sum("volume").cast("decimal(38,2)").alias("total_volume"),
            F.lit(None).cast("decimal(38,2)").alias("total_market_cap"),
            F.avg("volatility_20").alias("average_volatility")).withColumn("asset_class", F.lit("EQUITY")))

    if crypto is not None:
        counts["fact_crypto_market_snapshot"] = _merge_if_data(
            spark, crypto, table_name(catalog, gold_db, "fact_crypto_market_snapshot"),
            ["crypto_id", "vs_currency", "snapshot_date"], "months(snapshot_date)")
        counts["mart_crypto_performance"] = _merge_if_data(
            spark, crypto, table_name(catalog, gold_db, "mart_crypto_performance"),
            ["crypto_id", "vs_currency", "snapshot_date"], "months(snapshot_date)")
        overview_parts.append(crypto.groupBy(F.col("snapshot_date").alias("as_of_date")).agg(
            F.countDistinct("crypto_id").alias("asset_count"), F.avg("daily_return").alias("average_return"),
            F.sum("total_volume").cast("decimal(38,2)").alias("total_volume"),
            F.sum("market_cap").cast("decimal(38,2)").alias("total_market_cap"),
            F.avg("volatility_30").alias("average_volatility")).withColumn("asset_class", F.lit("CRYPTO")))

    if fx is not None:
        counts["fact_fx_daily_rates"] = _merge_if_data(
            spark, fx, table_name(catalog, gold_db, "fact_fx_daily_rates"),
            ["rate_date", "base_currency", "quote_currency", "source"], "months(rate_date)")
        counts["mart_fx_trends"] = _merge_if_data(
            spark, fx, table_name(catalog, gold_db, "mart_fx_trends"),
            ["rate_date", "base_currency", "quote_currency", "source"], "months(rate_date)")
        overview_parts.append(fx.groupBy(F.col("rate_date").alias("as_of_date")).agg(
            F.countDistinct(F.concat_ws("/", "base_currency", "quote_currency")).alias("asset_count"),
            F.avg("daily_change_pct").alias("average_return"),
            F.lit(None).cast("decimal(38,2)").alias("total_volume"),
            F.lit(None).cast("decimal(38,2)").alias("total_market_cap"),
            F.lit(None).cast("double").alias("average_volatility")).withColumn("asset_class", F.lit("FX")))

    if macro is not None:
        counts["fact_macro_observation"] = _merge_if_data(
            spark, macro, table_name(catalog, gold_db, "fact_macro_observation"),
            ["rbi_schema", "rbi_table", "indicator_code", "observation_period", "row_hash"], "months(observation_period)")
        latest_window = Window.partitionBy("rbi_schema", "rbi_table", "indicator_code").orderBy(
            F.col("observation_period").desc_nulls_last(), F.col("data_vintage").desc_nulls_last())
        latest = macro.withColumn("_rn", F.row_number().over(latest_window)).where(F.col("_rn") == 1).drop("_rn")
        counts["mart_macro_latest"] = _merge_if_data(
            spark, latest, table_name(catalog, gold_db, "mart_macro_latest"),
            ["rbi_schema", "rbi_table", "indicator_code"])

    if overview_parts:
        overview = overview_parts[0]
        for part in overview_parts[1:]:
            overview = overview.unionByName(part)
        overview = overview.select("as_of_date", "asset_class", "asset_count", "average_return",
                                   "total_volume", "total_market_cap", "average_volatility")
        counts["mart_asset_class_daily"] = _merge_if_data(
            spark, overview, table_name(catalog, gold_db, "mart_asset_class_daily"),
            ["as_of_date", "asset_class"], "months(as_of_date)")
    return counts


def process(args) -> dict[str, Any]:
    spark = create_spark("financial-silver-to-gold", args)
    started = run_metadata(args, "silver_to_gold")
    try:
        dimensions = build_dimensions(spark, args)
        equity = build_equity_gold(spark, args)
        crypto = build_crypto_gold(spark, args)
        fx = build_fx_gold(spark, args)
        macro = build_macro_gold(spark, args)
        facts_marts = build_marts(spark, args, equity, crypto, fx, macro)
        report = {**started, "status": "SUCCESS", "completed_at": datetime.now(timezone.utc).isoformat(),
                  "dimensions": dimensions, "facts_and_marts": facts_marts}
        audit_key = (f"audit/processing_runs/layer=gold/run_date={datetime.now(timezone.utc).date().isoformat()}"
                     f"/{started['job_run_id']}.json")
        write_json_audit(args.bucket, audit_key, report)
        report["audit_key"] = audit_key
        return report
    except Exception as exc:
        report = {**started, "status": "FAILED", "failed_at": datetime.now(timezone.utc).isoformat(),
                  "error": f"{type(exc).__name__}: {exc}"[:1500]}
        try:
            write_json_audit(args.bucket,
                f"audit/processing_runs/layer=gold/run_date={datetime.now(timezone.utc).date().isoformat()}"
                f"/{started['job_run_id']}.json", report)
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
