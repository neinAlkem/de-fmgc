{{ config(
    materialized='view'
) }}

WITH stg_stores AS(
    SELECT * FROM {{ source('raw', 'store_master') }}
), stores AS(
    SELECT *,
    md5(
        concat_ws('||', 
            store_id,
            store_name,
            area_code)
        ) AS hash_code,
    now() AS last_update
    FROM stg_stores
)
SELECT * FROM stores
