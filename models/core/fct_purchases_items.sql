{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'purchase_item_key',
    on_schema_change = 'sync_all_columns',
    tags = ['core', 'purchase_item']
  )
}}

WITH base_orders AS (

    SELECT
        purchase_key
        , time_order_received_utc
        , item_basket_description
    FROM {{ ref('stg_purchase_logs') }}

),

basket_items AS (

    -- One row per purchase × item
    SELECT
        o.purchase_key
        , o.time_order_received_utc
        , JSON_VALUE(item, '$.item_key') AS item_key
        , CAST(JSON_VALUE(item, '$.item_count') AS INT64) AS quantity
    FROM base_orders o,
    UNNEST(JSON_QUERY_ARRAY(item_basket_description)) AS item

),

valid_item_prices AS (

    -- Only valid, positive prices effective at purchase time
    SELECT
        b.purchase_key
        , b.item_key
        , h.price_including_vat AS item_price_at_purchase
        , ROW_NUMBER() OVER (
              PARTITION BY b.purchase_key, b.item_key
              ORDER BY h.valid_from_utc DESC
          ) AS rn
    FROM basket_items b
    JOIN {{ ref('dim_item_history') }} h
      ON b.item_key = h.item_key
     AND h.price_including_vat > 0
     AND h.valid_from_utc <= b.time_order_received_utc

),

promo_lookup AS (

    -- Item-level promo flag at purchase time
    SELECT DISTINCT
        b.purchase_key
        , b.item_key
        , TRUE AS was_on_promo
        , p.discount_in_percentage
    FROM basket_items b
    JOIN {{ ref('dim_promo') }} p
      ON b.item_key = p.item_key
     AND b.time_order_received_utc >= p.promo_start_utc
     AND b.time_order_received_utc <  p.promo_end_utc

)

SELECT
    -- Surrogate key
    CONCAT(b.purchase_key, '_', b.item_key) AS purchase_item_key

    , b.purchase_key
    , b.time_order_received_utc
    , b.item_key
    , b.quantity

    -- Validated price
    , p.item_price_at_purchase

    -- Promo attributes
    , COALESCE(pr.was_on_promo, FALSE) AS was_on_promo
    , pr.discount_in_percentage

FROM basket_items b
JOIN valid_item_prices p
  ON b.purchase_key = p.purchase_key
 AND b.item_key = p.item_key
 AND p.rn = 1

LEFT JOIN promo_lookup pr
  ON b.purchase_key = pr.purchase_key
 AND b.item_key = pr.item_key

{% if is_incremental() %}
WHERE b.time_order_received_utc >
      (SELECT MAX(time_order_received_utc) FROM {{ this }})
{% endif %}
