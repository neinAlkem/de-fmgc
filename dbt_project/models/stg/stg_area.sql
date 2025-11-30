{{ config(
    materialized='view'
) }}

WITH stg_area AS(
    SELECT * FROM {{ source('raw', 'area_master') }}
), area AS(
    SELECT 
        CAST(area_code AS VARCHAR) AS area_code,
        CAST(state AS VARCHAR) AS state,
        CAST(market AS VARCHAR) AS market,
        CAST(market_size AS VARCHAR) AS market_size,
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