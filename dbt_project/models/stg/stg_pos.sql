{{ config(
    materialized='view'
) }}

WITH stg_pos AS(
    SELECT *  FROM {{ source('raw', 'pos') }}
), pos AS(
    SELECT 
        CAST(transaction_id AS VARCHAR) AS transaction_id,
        CAST(date_purchased AS VARCHAR) AS date_purchased,
        CAST(area_code AS VARCHAR) AS area_code,
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(product_id AS VARCHAR) AS product_id,
        CAST(quantity AS VARCHAR) AS quantity,
        CAST(unit_price AS VARCHAR) AS unit_price,
        CAST(total_price AS VARCHAR) AS total_price,
        CAST(cogs AS VARCHAR) AS cogs,
        CAST(inventory_latest AS VARCHAR) AS inventory_latest,
        CAST(inventory_after AS VARCHAR) AS inventory_after,
    md5(
        concat_ws('||', 
            transaction_id,
            date_purchased,
            area_code,
            store_id,
            product_id,
            quantity,
            unit_price,
            total_price,
            cogs,
            inventory_latest,
            inventory_after)
        ) AS hash_code,
    now() AS last_update
    FROM stg_pos
)
SELECT * FROM pos
