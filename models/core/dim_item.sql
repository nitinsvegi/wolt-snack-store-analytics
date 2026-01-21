{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'item_key',
    on_schema_change = 'sync_all_columns',
    tags = ['core', 'item']
  )
}}

WITH latest_attributes AS (

  -- Take attributes from the most recent log per item
  SELECT
      item_key
      , brand_name
      , category
      , item_name_en
      , weight_in_grams
      , valid_from_utc AS last_seen_in_logs_utc
      , ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
  FROM {{ ref('dim_item_history') }}
),

last_positive_price AS (

  -- Search the ENTIRE history for the most recent positive price
  SELECT
      item_key
      , COALESCE(price_including_vat, 0) AS current_price_including_vat
      , CASE WHEN price_including_vat IS NULL 
             THEN TRUE 
             ELSE FALSE 
        END AS has_no_positive_price
      , valid_from_utc AS price_effective_from_utc
      , ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
  FROM {{ ref('dim_item_history') }}
  WHERE price_including_vat > 0
)

SELECT
    a.item_key

    -- product attributes from latest record
    , a.brand_name
    , a.category
    , a.item_name_en
    , a.weight_in_grams

    -- price from last known positive value in history
    , p.current_price_including_vat
    , p.has_no_positive_price
    , p.price_effective_from_utc

    -- metadata
    , a.last_seen_in_logs_utc

FROM latest_attributes a
LEFT JOIN last_positive_price p
  ON a.item_key = p.item_key
  AND p.rn = 1
WHERE TRUE
  AND a.rn = 1

{% if is_incremental() %}
AND a.item_key NOT IN (
   SELECT item_key FROM {{ this }}
)
{% endif %}