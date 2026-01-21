{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'promo_key',
    on_schema_change = 'sync_all_columns',
    tags = ['core','promo','dimension']
  )
}}

WITH parsed AS (

  SELECT
      item_key
      -- Convert dates to timestamps with clear business meaning
      , TIMESTAMP(promo_start_date) AS promo_start_utc
      -- Promotion is NOT valid from midnight on this date
      , TIMESTAMP(promo_end_date) AS promo_end_utc
      , LOWER(TRIM(promo_type)) AS promo_type
      , CAST(discount_in_percentage AS INT64) AS discount_in_percentage
  FROM {{ ref('stg_promos') }} 
),

enriched AS (
  SELECT
      -- surrogate key: one row per item + promo window
      CONCAT(item_key, '_',
             FORMAT_TIMESTAMP('%Y%m%d', promo_start_utc), '_',
             FORMAT_TIMESTAMP('%Y%m%d', promo_end_utc)
      ) AS promo_key
      , item_key
      , promo_start_utc
      , promo_end_utc
      , promo_type
      , discount_in_percentage
      -- useful business flags
      , (CURRENT_TIMESTAMP() >= promo_start_utc
         AND CURRENT_TIMESTAMP() < promo_end_utc
        ) AS is_current_promo
      , DATE_DIFF(DATE(promo_end_utc),DATE(promo_start_utc), DAY) AS promo_length_days
  FROM parsed
)

SELECT *
FROM enriched

{% if is_incremental() %}
WHERE promo_key NOT IN (
  SELECT promo_key
  FROM {{ this }}
)
{% endif %}
