{{ config(
    materialized='view'
) }}

WITH stg_stores AS(
    SELECT * FROM {{ source('raw', 'store_master') }}
), stores AS(
    SELECT 
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(store_name AS VARCHAR) AS store_name,
        CAST(area_code AS VARCHAR) AS area_code, 
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
