-- Latest observation and latest known vintage for each registered RBI indicator.
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY rbi_schema, rbi_table, indicator_code
        ORDER BY observation_period DESC, data_vintage DESC
    ) AS rn
    FROM lakehouse_gold.fact_macro_observation
)
SELECT rbi_schema, rbi_table, indicator_code, indicator_name,
       observation_period, available_date, data_vintage,
       observation_value, unit, source_row_json
FROM ranked
WHERE rn = 1
ORDER BY indicator_name;
