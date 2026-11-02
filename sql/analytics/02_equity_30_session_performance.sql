-- Uses 30 available trading observations, not 30 calendar days.
WITH rolling AS (
    SELECT instrument_key, trading_date, close_price,
           FIRST_VALUE(close_price) OVER (
               PARTITION BY instrument_key ORDER BY trading_date
               ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
           ) AS close_30_sessions_ago,
           ROW_NUMBER() OVER (PARTITION BY instrument_key ORDER BY trading_date DESC) AS latest_rank
    FROM lakehouse_gold.fact_equity_daily_ohlcv
), latest AS (
    SELECT *, close_price / NULLIF(close_30_sessions_ago, 0) - 1 AS return_30_sessions
    FROM rolling WHERE latest_rank = 1
)
SELECT l.instrument_key, d.symbol, d.company_name, l.trading_date,
       l.close_price, l.return_30_sessions
FROM latest l
LEFT JOIN lakehouse_gold.dim_instrument d ON l.instrument_sk = d.instrument_sk
ORDER BY return_30_sessions DESC
LIMIT 10;
