{{
  config(
    materialized = 'incremental',
    unique_key= 'hash_code'
    )
}}

WITH dbo_area AS(
    SELECT * FROM {{ source('stg', 'stg_area') }}
), rename AS(
    SELECT
        CAST(area_code AS INT ) AS area_code,
        CAST(state AS VARCHAR) AS state,
        CAST(market AS VARCHAR) AS market,
        CAST(market_size AS VARCHAR) AS market_size,
        CAST(hash_code AS VARCHAR) AS hash_code,
        CAST(last_update AS TIMESTAMP) AS last_update    
    FROM dbo_area
    {% if is_incremental() %}
    WHERE hash_code NOT IN (SELECT hash_code FROM {{ this }})
    {% endif %}
) 
SELECT * FROM rename 