{{ config(
    materialized='view'
) }}

WITH stg_products AS(
    SELECT * FROM {{ source('raw', 'coffee_master') }}
), products AS(
    SELECT *,
    md5(
        concat_ws('||', 
            product_id,
            product_type,
            product,
            type,
            price,
            cogs)
        ) AS hash_code,
    now() AS last_update
    FROM stg_products
)
SELECT * FROM products


