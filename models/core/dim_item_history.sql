{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='item_history_key',
        on_schema_change='sync_all_columns',
        tags=['core','item','history']
    )
}}

WITH parsed AS (

    SELECT
        log_item_id
        , item_key
        , time_log_created_utc AS valid_from_utc
        , SAFE_CAST(
            JSON_VALUE(payload_json, '$.time_item_created_in_source_utc')
            AS TIMESTAMP
        ) AS time_item_created_in_source_utc

        , COALESCE(JSON_VALUE(payload_json, '$.brand_name'), 'unknown_brand') AS brand_name
        , JSON_VALUE(payload_json, '$.item_category') AS category

        -- take the English product name
        , (
            SELECT JSON_VALUE(n, '$.value')
            FROM UNNEST(JSON_QUERY_ARRAY(payload_json, '$.name')) n
            WHERE JSON_VALUE(n, '$.lang') = 'en'
            LIMIT 1
        ) AS item_name_en

        -- price and VAT
        , SAFE_CAST(
            JSON_VALUE(
                JSON_QUERY_ARRAY(payload_json, '$.price_attributes')[SAFE_OFFSET(0)],
                '$.product_base_price'
            ) AS FLOAT64
          ) AS price_including_vat

        , SAFE_CAST(
            JSON_VALUE(
                JSON_QUERY_ARRAY(payload_json, '$.price_attributes')[SAFE_OFFSET(0)],
                '$.vat_rate_in_percent'
            ) AS INT64
          ) AS vat_percentage

        -- weight in grams  
        , SAFE_CAST(
            JSON_VALUE(payload_json, '$.weight_in_grams')
            AS INT64
        ) AS weight_in_grams


        , payload_json

    FROM {{ ref('stg_item_logs') }}
),

windowed AS (

    SELECT
        CONCAT(
            item_key, '_', 
            FORMAT_TIMESTAMP('%Y%m%d%H%M%S', valid_from_utc), '_',
            COALESCE(CAST(price_including_vat AS STRING), 'missing')
        ) AS item_history_key
        , log_item_id
        , item_key
        , brand_name
        , category
        , item_name_en
        , price_including_vat
        , vat_percentage
        , CASE 
            WHEN price_including_vat IS NULL THEN 'missing'
            WHEN price_including_vat < 0 THEN 'negative'
            WHEN price_including_vat > 0 THEN 'positive'
            ELSE 'zero'
          END AS price_type
        , weight_in_grams
        , time_item_created_in_source_utc
        , valid_from_utc

        , LEAD(valid_from_utc) OVER (
            PARTITION BY item_key
            ORDER BY valid_from_utc, log_item_id
          ) AS valid_to_utc
        
        , payload_json

    FROM parsed
)

SELECT *
FROM windowed

{% if is_incremental() %}
WHERE valid_from_utc >
      (SELECT MAX(valid_from_utc) FROM {{ this }})
{% endif %}