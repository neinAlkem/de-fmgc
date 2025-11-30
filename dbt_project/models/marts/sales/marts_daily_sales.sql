{{
  config(
    materialized = 'table',
    )
}}

WITH total_profit AS (
    SELECT
        t1.transaction_id,
        t1.total_price - (t2.cogs * t1.quantity) AS profit
    FROM {{ source('dbo', 'dbo_pos') }} t1
    INNER JOIN {{ source('dbo', 'dbo_products') }} t2
        ON t1.product_id = t2.product_id
), 

base_table AS( 
    SELECT 
        t1.date_purchases AS date, 
        t3.store_id,
        t3.store_name, 
        t4.state,
        COUNT(t1.transaction_id) AS total_transaction, 
        SUM(t1.quantity) AS total_sales, 
        SUM(t1.total_price) AS total_revenue, 
        SUM(t2.profit) AS gross_revenue
    FROM 
        {{ source('dbo', 'dbo_pos') }} AS t1
    INNER JOIN
        total_profit AS t2 
        ON t2.transaction_id = t1.transaction_id
    INNER JOIN 
        {{ source('dbo', 'dbo_stores') }} AS t3 
        ON t3.store_id = t1.store_id
    INNER JOIN 
        {{ source('dbo', 'dbo_area') }} AS t4 
        ON t4.area_code = t1.area_code
    INNER JOIN 
        {{ source('dbo', 'dbo_products') }} 
        AS t5 ON t5.product_id = t1.product_id
    GROUP BY 
        t1.date_purchases, 
        t3.store_id,
        t3.store_name, 
        t4.state
), 

lags AS (
    SELECT 
        *,
        {{ get_lags('total_sales', 'lag(total_sales, 1, 0) OVER (PARTITION BY store_id ORDER BY date)') }} AS lag_1day,
        {{ get_lags('total_sales', 'lag(total_sales, 7, 0) OVER (PARTITION BY store_id ORDER BY date)') }} AS lag_7day
    FROM base_table
), 

rolling_sum AS (
    SELECT 
        *,
        CAST(SUM(total_sales) OVER (PARTITION BY store_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS FLOAT) AS weekly_rolling_sum,
        AVG(total_sales) OVER (PARTITION BY store_id ORDER BY date ROWS BETWEEN 6  PRECEDING AND CURRENT ROW) AS weekly_rolling_avg
    FROM lags
)
SELECT 
    * 
FROM 
    rolling_sum
ORDER BY    
    date, 
    store_id,
    store_name, 
    state

