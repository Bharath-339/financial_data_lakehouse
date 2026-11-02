-- Latest trading-day close for every tracked equity.
WITH latest_day AS (
    SELECT MAX(trading_date) AS trading_date
    FROM lakehouse_gold.fact_equity_daily_ohlcv
)
SELECT f.instrument_key, d.symbol, d.company_name, d.exchange,
       f.trading_date, f.open_price, f.high_price, f.low_price,
       f.close_price, f.volume, f.daily_return, f.sma_7, f.sma_20,
       f.volatility_20
FROM lakehouse_gold.fact_equity_daily_ohlcv f
LEFT JOIN lakehouse_gold.dim_instrument d
  ON f.instrument_sk = d.instrument_sk
CROSS JOIN latest_day x
WHERE f.trading_date = x.trading_date
ORDER BY f.volume DESC;
