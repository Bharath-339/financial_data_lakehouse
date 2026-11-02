# Operations Runbook

## Daily data flow

1. Airflow invokes the public Upstox BOD instrument master once per run, one Lambda task per tracked Upstox instrument, plus CoinGecko and Frankfurter tasks. The BOD feed is public gzip JSON and does not use the OAuth token; historical candle extraction does.
2. Lambda validates the event, acquires the source checkpoint lease, calls the provider through the shared HTTP/retry/rate-limit client, writes gzip Bronze envelopes, and writes an ingestion audit manifest.
3. Lambda advances the date watermark only after extraction succeeds and only when the event's `advance_watermark` flag permits it.
4. Airflow runs one Bronze-to-Silver Glue job per source/entity, scoped to the current Asia/Kolkata `extract_date`, including `upstox/instrument_master`.
5. The Gold SCD2 `dim_instrument` is refreshed from the latest instrument master snapshot. `config/reference/instruments.csv` enriches fields the API does not reliably supply (for example sector) and provides reviewed fallback values.
6. The Silver job validates the selected Bronze envelope slice (gzip/JSON, required metadata, source/entity path), normalizes the payload, writes invalid rows to quarantine, and uses Iceberg `MERGE` for idempotent upserts.
7. Airflow runs Silver-to-Gold once all daily Silver jobs finish.
8. Gold rebuilds SCD2 dimensions, facts and analytical marts, writes a processing audit, and commits Iceberg snapshots.

## Recover a failed ingestion

- Inspect `audit/ingestion_runs/` and CloudWatch Lambda logs using the Lambda `run_id`. The AWS processing stack exposes alarms for Lambda errors, throttles and high duration; the initial alarms do not notify anyone until an SNS action is added.
- When the Upstox token expires, obtain a new token through the provider's user-consent flow and run `scripts/set_upstox_token.sh` after sourcing the appropriate AWS/Floci environment.
- A FAILED Lambda run does not advance its watermark; the lease is released by the handler's failure path.
- The first scheduled run uses the configured `*_BOOTSTRAP_START_DATE` value; after a successful run, subsequent invocations use the DynamoDB watermark with an overlap window.
- Reinvoke the same source/entity/instrument event after the failure cause is resolved. The raw path contains a new run ID, so raw data is retained.
- If an earlier extraction succeeded but a later transformation failed, do not call the provider again. Rerun Bronze-to-Silver for that source/entity, then Silver-to-Gold.

## Recover a failed Silver/Gold transformation

- Inspect `audit/processing_runs/layer=silver` or `layer=gold`.
- Correct schema/mapping or code and rerun the same Spark job.
- Silver and Gold use deterministic `MERGE` keys; repeated runs should not add duplicates.
- Inspect `quarantine/<source>/run_id=<spark-job-run-id>/` for row-level issues.
- For RBI rows with `dq_status='REVIEW'`, inspect `source_row_json`, add a reviewed mapping to `config/reference/macro_indicators.csv`, sync the reference directory, rerun Silver, then Gold.

## Historical backfills

- Use `examples/airflow/historical_backfill.py` for a bounded event with required `source`, `entity`, `start_date`, `end_date`, and provider key (instrument or schema/table) where applicable.
- Backfills do not advance the watermark by default. Set `advance_watermark=true` only when the backfill covers the latest complete range and is intended to establish/advance the checkpoint.
- Upstox instrument metadata is captured as daily BOD snapshots. The provider recommends `instrument_key` as the stable identifier; do not join facts by `exchange_token`, which may be reused. Metadata before the first collected BOD snapshot is not historically known, so initial attributes are backcast to the earliest tracked candle date until actual master history accumulates.
- Upstox V3 daily date windows must respect the provider's maximum range and are split by the ingestion connector.
- After a backfill, run the matching Bronze-to-Silver job and then Silver-to-Gold.

## Data corrections

- Equity corrections are re-extracted through the configured overlap window and merged by instrument/date/interval/source.
- Crypto daily snapshots are keyed by coin/currency/date. A later snapshot on the same date replaces the earlier canonical daily snapshot in Silver/Gold; Bronze retains each raw pull.
- FX is keyed by rate date/base/quote/source.
- RBI revisions are append-preserved in Silver by row hash and vintage. `mart_macro_latest` selects the latest period and latest vintage for the indicator.

## Stale or invalid source data

- Check provider HTTP status, quota, authentication secret, ingestion audit object count and `last_successful_date` in DynamoDB.
- Do not advance watermarks on empty Upstox responses unless a deliberate event uses `allow_empty=true` after confirming the market calendar.
- An absent weekend/holiday equity candle is not automatically a data-quality failure. Compare missing dates with the exchange calendar.
- Frankfurter does not necessarily publish on weekends/holidays; preserve rate date and do not manufacture a new observation for every calendar day.

## Iceberg maintenance

- Weekly maintenance compacts eligible data files and expires snapshots older than the configured retention while retaining at least seven snapshots.
- The shipped DAG passes 30 days; the Glue job default is 7 days. Choose one operational policy and set it explicitly.
- Do not run snapshot expiration while a long-running backfill or reader depends on old snapshots.
- Verify current snapshots and recent queries before reducing retention. Snapshot expiration limits time travel/recovery.

## RBI source onboarding checklist

1. Invoke `events/rbi_catalogue.json` with a search query, then inspect the raw Bronze response.
2. Run the `table_catalogue` Bronze-to-Silver job and inspect `lakehouse_silver.rbi_table_catalogue` and its `catalogue_json`.
3. Invoke `table_rows` for a candidate schema/table. The Lambda writes the table metadata response as a separate `table_metadata` Bronze envelope during this extraction.
4. Run Bronze-to-Silver separately for `rbi/table_metadata` and inspect `lakehouse_silver.rbi_table_metadata.metadata_json`.
5. Fetch a small sample range with pagination and determine exact observation-period, indicator, value, unit and publication-date column names.
6. Add an explicit reviewed entry to `config/reference/macro_indicators.csv` and/or extend `lakehouse.quality_rules.rbi_field_candidates()` only for aliases valid across the provider, not for a one-off table guess.
7. Re-run Silver and confirm `dq_status='VALID'` for the mapped rows before including them in Gold.

## Local Floci troubleshooting

- Confirm `curl -fsS http://localhost:4566/_floci/health` succeeds.
- For boto3/CLI calls, use local `AWS_ENDPOINT_URL=http://localhost:4566` and path-style S3 addressing.
- For calls from a Lambda Docker container, use a host/container-reachable endpoint; `localhost` inside the Lambda container points to that container, not the host Floci process.
- Confirm `.env.floci.generated` matches the CloudFormation output bucket and DynamoDB table. It overrides stack-created identifiers from the user's `.env.floci` configuration.
- If Spark cannot resolve `glue_catalog`, create the Silver/Gold Glue databases with `scripts/bootstrap_catalog.py` and inspect `aws glue get-databases` against the Floci endpoint.
- If Spark can write files but Floci Athena cannot query the Iceberg table, inspect `aws glue get-table` and ensure `metadata_location` refers to an object accessible by the Athena/DuckDB sidecar. Do not infer successful Iceberg integration from only successful writes to S3.

## Escalation information to capture

For any failure, retain: provider/source/entity, run ID, Lambda request ID, date range, HTTP status (without auth headers), raw object keys, Spark job run ID, target Iceberg table, audit manifest key, and exact error/stack trace. Never attach API tokens or secret values to issue reports.
