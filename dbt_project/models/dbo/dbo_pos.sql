{{
  config(
    materialized = 'incremental',
    unique_key= 'hash_code'
    )
}}

WITH dbo_pos AS (
    SELECT * FROM {{ source('stg', 'stg_pos') }}
), rename AS( 
    SELECT
        CAST(transaction_id AS VARCHAR) AS transaction_id,
        CAST(date_purchased AS DATE) AS date_purchases,
        CAST(area_code AS CHAR(3)) AS area_code,
        CAST(store_id AS CHAR(8)) AS store_id,
        CAST(product_id AS INT) AS product_id, 
        CAST(quantity as INT) as quantity,
        CAST(unit_price AS FLOAT) AS unit_price,
        CAST(total_price AS FLOAT) AS total_price,
        CAST(cogs AS FLOAT) AS cogs,
        CAST(inventory_latest AS INT) AS inventory_latest, 
        CAST(inventory_after AS INT) AS inventory_after,
        CAST(hash_code AS VARCHAR) AS hash_code,
        CAST(last_update AS TIMESTAMP) AS last_update    
    FROM dbo_pos
    {% if is_incremental() %}
    WHERE hash_code NOT IN (SELECT hash_code FROM {{ this }})
    {% endif %}
)
SELECT * FROM rename 
