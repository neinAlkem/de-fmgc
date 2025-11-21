{{
  config(
    materialized = 'incremental',
    unique_key= 'hash_code'
    )
}}

WITH dbo_products AS(
    SELECT * FROM {{ source('stg', 'stg_products') }}
), rename AS(
    SELECT
        CAST(product_id AS INT) AS product_id,
        CAST(product_type AS VARCHAR) AS product_type,
        CAST(product AS VARCHAR) AS product,
        CAST(type AS VARCHAR) AS variety,
        CAST(price AS FLOAT) AS price,
        CAST(cogs AS FLOAT) AS cogs,
        CAST(hash_code AS VARCHAR) AS hash_code,
        CAST(last_update AS TIMESTAMP) AS last_update    
        FROM dbo_products
    {% if is_incremental() %}
    WHERE hash_code NOT IN (SELECT hash_code FROM {{ this }}) 
    {% endif %}
) 
SELECT * FROM rename