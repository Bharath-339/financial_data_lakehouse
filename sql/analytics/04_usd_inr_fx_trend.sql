SELECT rate_date, exchange_rate, daily_change_pct, rate_sma_30
FROM lakehouse_gold.mart_fx_trends
WHERE base_currency = 'USD' AND quote_currency = 'INR'
ORDER BY rate_date DESC
LIMIT 365;
