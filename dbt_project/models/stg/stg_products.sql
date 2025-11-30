{{ config(
    materialized='view'
) }}

WITH stg_products AS(
    SELECT * FROM {{ source('raw', 'coffee_master') }}
), products AS(
    SELECT 
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(product_type AS VARCHAR) AS product_type,
        CAST(product AS VARCHAR) AS product,
        CAST(type AS VARCHAR) AS type,
        CAST(price AS VARCHAR) AS price,
        CAST(cogs AS VARCHAR) AS cogs,
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


