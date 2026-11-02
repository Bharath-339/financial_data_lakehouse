SELECT as_of_date, asset_class, asset_count, average_return,
       total_volume, total_market_cap, average_volatility
FROM lakehouse_gold.mart_asset_class_daily
ORDER BY as_of_date DESC, asset_class;
