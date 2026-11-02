"""Iceberg compaction and conservative snapshot-retention operations."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

from lakehouse.spark.common import create_spark, parse_args, table_name, write_json_audit

TABLES = {
    "silver": ["equity_daily_ohlcv", "upstox_instrument_master", "crypto_market_snapshots", "fx_daily_rates", "macro_observations",
               "rbi_table_catalogue", "rbi_table_metadata"],
    "gold": [
        "dim_instrument", "dim_crypto_asset", "dim_currency", "dim_macro_indicator", "dim_date", "dim_data_source",
        "fact_equity_daily_ohlcv", "fact_crypto_market_snapshot", "fact_fx_daily_rates", "fact_macro_observation",
        "mart_equity_returns", "mart_crypto_performance", "mart_fx_trends", "mart_macro_latest", "mart_asset_class_daily",
    ],
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-retention-days", type=int,
                        default=int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "7")))
    parser.add_argument("--target-file-size-bytes", type=int,
                        default=int(os.environ.get("ICEBERG_TARGET_FILE_SIZE_BYTES", "134217728")))
    args, remaining = parser.parse_known_args(argv)
    if args.snapshot_retention_days < 7:
        parser.error("Snapshot retention must be at least 7 days for this project")
    job_args = parse_args(remaining)
    spark = create_spark("financial-iceberg-maintenance", job_args)
    results = []
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=args.snapshot_retention_days)).strftime("%Y-%m-%d %H:%M:%S")
        database_names = {"silver": job_args.silver_database, "gold": job_args.gold_database}
        for layer, tables in TABLES.items():
            database = database_names[layer]
            for table in tables:
                identifier = table_name(job_args.catalog, database, table)
                try:
                    if not spark.catalog.tableExists(f"{job_args.catalog}.{database}.{table}"):
                        continue
                    spark.sql(
                        f"CALL {job_args.catalog}.system.rewrite_data_files(" 
                        f"table => '{database}.{table}', options => map('target-file-size-bytes','{args.target_file_size_bytes}'))"
                    )
                    spark.sql(
                        f"CALL {job_args.catalog}.system.expire_snapshots(" 
                        f"table => '{database}.{table}', older_than => TIMESTAMP '{cutoff}', retain_last => 7)"
                    )
                    results.append({"table": identifier, "status": "SUCCESS"})
                except Exception as exc:
                    results.append({"table": identifier, "status": "FAILED", "error": str(exc)[:500]})
        failed_tables = [item for item in results if item["status"] == "FAILED"]
        report = {
            "status": "FAILED" if failed_tables else "SUCCESS",
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_retention_days": args.snapshot_retention_days,
            "target_file_size_bytes": args.target_file_size_bytes,
            "tables": results,
        }
        key = f"audit/table_maintenance/{datetime.now(timezone.utc).date().isoformat()}/maintenance.json"
        write_json_audit(job_args.bucket, key, report)
        print(json.dumps(report, indent=2))
        if failed_tables:
            raise RuntimeError(f"Iceberg maintenance failed for {len(failed_tables)} table(s): {failed_tables}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
