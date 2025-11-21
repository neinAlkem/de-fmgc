{{
  config(
    materialized = 'incremental',
    unique_key= 'hash_code'
    )
}}


WITH dbo_store AS(
    SELECT * FROM {{ source('stg', 'stg_stores') }}
), rename AS(
    SELECT
        CAST(store_id AS VARCHAR) AS store_id,
        CAST(store_name AS VARCHAR) AS store_name,
        CAST(area_code AS CHAR(3)) AS area_code,
        CAST(hash_code AS VARCHAR) AS hash_code,
        CAST(last_update AS TIMESTAMP) AS last_update    
    FROM dbo_store
    {% if is_incremental() %}
    WHERE hash_code NOT IN (SELECT hash_code FROM {{ this }})
    {% endif %}
)
SELECT * FROM rename