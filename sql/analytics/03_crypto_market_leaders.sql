-- Latest observed crypto snapshot in each reporting currency.
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY vs_currency ORDER BY snapshot_date DESC, snapshot_timestamp DESC
    ) AS latest_rank
    FROM lakehouse_gold.fact_crypto_market_snapshot
)
SELECT crypto_id, symbol, name, vs_currency, snapshot_date,
       current_price, market_cap, total_volume, market_cap_rank,
       daily_return, volatility_30
FROM ranked
WHERE latest_rank = 1
ORDER BY vs_currency, market_cap DESC
LIMIT 100;
