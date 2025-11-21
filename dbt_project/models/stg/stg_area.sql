{{ config(
    materialized='table'
) }}

WITH stg_area AS(
    SELECT * FROM {{ source('raw', 'area_master') }}
), area AS(
    SELECT * ,
    md5(
        concat_ws('||', 
            area_code, 
            state, 
            market, 
            market_size)
        ) AS hash_code,
    now() AS last_update 
    FROM stg_area
) 
SELECT * FROM area