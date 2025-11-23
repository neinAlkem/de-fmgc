{{
  config(
    materialized = 'table',
    )
}}

SELECT 
    t1.date_purchases AS date,
    t1.product_id,
    t2.product AS product_name,
    SUM(t1.quantity) AS total_sales
FROM
    {{ source('dbo', 'dbo_pos') }} t1
LEFT JOIN
    {{ source('dbo', 'dbo_products') }} t2
    ON t1.product_id = t2.product_id
GROUP BY 
    t1.date_purchases, 
    t1.product_id, 
    t2.product
ORDER BY
    t1.date_purchases, 
    t1.product_id, 
    t2.product