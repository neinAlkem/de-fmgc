{{ config(
    materialized='view'
) }}

WITH stg_pos AS(
    SELECT *  FROM {{ source('raw', 'pos') }}
), pos AS(
    SELECT *,
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
