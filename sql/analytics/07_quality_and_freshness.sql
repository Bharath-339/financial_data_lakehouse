-- Silver quality status and latest ingested record per provider table.
SELECT 'equity_daily_ohlcv' AS table_name,
       MAX(ingested_at) AS latest_ingestion,
       COUNT(*) AS total_rows,
       COUNT_IF(trading_date IS NULL OR close_price IS NULL) AS invalid_rows
FROM lakehouse_silver.equity_daily_ohlcv
UNION ALL
SELECT 'crypto_market_snapshots', MAX(ingested_at), COUNT(*),
       COUNT_IF(crypto_id IS NULL OR current_price IS NULL)
FROM lakehouse_silver.crypto_market_snapshots
UNION ALL
SELECT 'fx_daily_rates', MAX(ingested_at), COUNT(*),
       COUNT_IF(exchange_rate IS NULL OR exchange_rate <= 0)
FROM lakehouse_silver.fx_daily_rates
UNION ALL
SELECT 'macro_observations', MAX(ingested_at), COUNT(*),
       COUNT_IF(dq_status <> 'VALID')
FROM lakehouse_silver.macro_observations;
UNION ALL
SELECT 'rbi_table_catalogue', MAX(ingested_at), COUNT(*),
       COUNT_IF(dq_status <> 'VALID')
FROM lakehouse_silver.rbi_table_catalogue
UNION ALL
SELECT 'rbi_table_metadata', MAX(ingested_at), COUNT(*),
       COUNT_IF(dq_status <> 'VALID')
FROM lakehouse_silver.rbi_table_metadata;
