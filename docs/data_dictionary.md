# Financial Data Lakehouse – Data Dictionary

## Logical databases

- `lakehouse_silver`: cleaned, source-aligned observations. Silver keeps source/provider identifiers, ingestion lineage and data quality status.
- `lakehouse_gold`: dimensions, facts and query-oriented marts. Tables are Iceberg-managed and registered in the Glue Data Catalog.

The default Iceberg warehouse location is `s3://<bucket>/warehouse/`. The logical database/table names distinguish Silver and Gold; the physical Iceberg table files and metadata are kept under the warehouse prefix. Bronze objects remain under `bronze/`; quarantined outputs under `quarantine/`; processing/ingestion manifests under `audit/`.

## Silver tables

### `upstox_instrument_master`

**Grain:** one Upstox `instrument_key` per daily BOD master snapshot date.

Important columns: `instrument_key`, `master_date`, `segment`, `trading_symbol`, `name`, `short_name`, `exchange`, `isin`, `instrument_type`, `security_type`, `exchange_token`, `lot_size`, `tick_size`, `expiry_raw`, `strike_price`, `underlying_symbol`, `underlying_key`, `source_record_json`, `record_hash`, and ingestion lineage. `instrument_key` is the stable provider identifier; `exchange_token` may be reused and should not be used as the warehouse business key.

### `equity_daily_ohlcv`

**Grain:** one provider instrument, trading date, candle interval and source.

Important columns: `instrument_key`, `trading_date`, `candle_timestamp`, `interval`, `open_price`, `high_price`, `low_price`, `close_price`, `volume`, `open_interest`, `source`, `ingestion_run_id`, `ingested_at`, `extract_date`, `bronze_source_file`.

### `crypto_market_snapshots`

**Grain:** one CoinGecko asset, quote currency and snapshot date. Repeated same-day pulls are deduplicated by latest provider update/ingestion timestamp.

Important columns: `crypto_id`, `symbol`, `name`, `vs_currency`, `snapshot_timestamp`, `snapshot_date`, `current_price`, `market_cap`, `total_volume`, `circulating_supply`, `total_supply`, `max_supply`, `market_cap_rank`, lineage columns.

### `fx_daily_rates`

**Grain:** one Frankfurter base/quote pair and rate date.

Important columns: `rate_date`, `base_currency`, `quote_currency`, `exchange_rate`, `source`, lineage columns.

### `rbi_table_catalogue`

**Grain:** one discovered RBI catalogue item and raw-row hash from a catalogue response.

Important columns: `rbi_schema`, `rbi_table`, `table_title`, `table_description`, `catalogue_json`, `row_hash`, `dq_status`, `dq_reason`, and ingestion lineage. Catalogue field names vary by DBIE response; the full JSON is retained so mappings can be added without losing source detail. Rows without a stable table identifier are retained with `dq_status='REVIEW'` and quarantined.

### `rbi_table_metadata`

**Grain:** one metadata document version for a schema/table, identified by `metadata_hash`.

Important columns: `rbi_schema`, `rbi_table`, `metadata_json`, `metadata_hash`, `dq_status`, `dq_reason`, and ingestion lineage. A changed metadata document creates a new version rather than overwriting the previous metadata hash.

### `macro_observations`

**Grain:** one RBI table row and row hash. Multiple vintages are retained because revised macro observations must not overwrite the original value.

Important columns: `rbi_schema`, `rbi_table`, `indicator_code`, `observation_period`, `observation_period_raw`, `observation_value`, `value_raw`, `unit`, `available_date`, `data_vintage`, `row_hash`, `source_row_json`, `dq_status`, `dq_reason`, lineage columns.

RBI table mappings are provider-table-specific. Unmapped rows are retained in Silver with `dq_status='REVIEW'` and copied to quarantine. They are not promoted to Gold facts until mappings and values are reviewed.

## Gold dimensions

| Table | Business key | History |
|---|---|---|
| `dim_instrument` | `instrument_key` | SCD Type 2 |
| `dim_crypto_asset` | `crypto_id` | SCD Type 2 |
| `dim_currency` | `currency_code` | Type 1 reference lookup |
| `dim_macro_indicator` | `rbi_schema + rbi_table + indicator_code` | Type 1 reviewed mapping |
| `dim_date` | `date_key` (`yyyyMMdd`) | Static calendar |
| `dim_data_source` | `source_id` | Static provider lookup |

SCD2 dimensions contain a surrogate key, `attribute_hash`, `effective_from`, `effective_to`, and `is_current`. Fact joins use the observation date between effective bounds, not simply the current row. Since validity is date-grained, multiple attribute changes for the same business key on one processing date collapse to the latest same-day version; intraday dimension history would require timestamp-grained validity columns.

## Gold facts and marts

| Table | Grain / purpose | Iceberg business key |
|---|---|---|
| `fact_equity_daily_ohlcv` | One equity candle | instrument + trading date + interval + source |
| `fact_crypto_market_snapshot` | One asset/currency/day snapshot | crypto ID + quote currency + snapshot date |
| `fact_fx_daily_rates` | One currency pair/rate date | rate date + base + quote + source |
| `fact_macro_observation` | One indicator observation vintage | schema + table + indicator + period + row hash |
| `mart_equity_returns` | Equity price/return/rolling metric history | Same as equity fact |
| `mart_crypto_performance` | Crypto price/return/rolling metric history | Same as crypto fact |
| `mart_fx_trends` | FX rate/change/moving average history | Same as FX fact |
| `mart_macro_latest` | Latest observation/vintage for an indicator | schema + table + indicator |
| `mart_asset_class_daily` | Daily aggregate by asset class | as-of date + asset class |

## Financial metric definitions

- `daily_return = current_close / previous_close - 1` (and analogous calculations for crypto price / FX rate).
- `sma_7`, `sma_20`, `price_sma_7`, `price_sma_30`, `rate_sma_30` are rolling means over available observations, not necessarily calendar days.
- `volatility_20` and `volatility_30` use sample standard deviation of daily returns over the trailing observation window. These are not annualised.
- Equity daily observations use the exchange-local date from the Upstox timestamp. The UTC timestamp is retained separately.
- Macro facts keep `observation_period`, `available_date` and `data_vintage` distinct to support point-in-time analysis and revision tracking.

## Reviewed RBI mapping configuration

Edit `config/reference/macro_indicators.csv` only after inspecting the source table metadata and sample rows. Columns:

`rbi_schema,rbi_table,indicator_code,indicator_name,unit`

If the API table provides a single indicator per table, set `indicator_code` to the generated/table code used by the Silver transform. If a table contains multiple indicators, provide the exact stable code from the API row. Do not label an unknown row with a guessed macro indicator.
