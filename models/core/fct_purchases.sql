{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'purchase_key',
    on_schema_change = 'sync_all_columns',
    tags = ['core', 'fact', 'purchase']
  )
}}

WITH base_orders AS (

    SELECT
        purchase_key
        , time_order_received_utc AS time_order_received_utc
        , DATE(time_order_received_utc) AS time_order_received_date
        , customer_key
        , delivery_distance_line_meters AS delivery_distance_meters
        , wolt_service_fee
        , courier_base_fee
        , total_basket_value
        , item_basket_description
    FROM {{ ref('stg_purchase_logs') }}
),

basket_items AS (

    -- explode the basket JSON
    SELECT
        o.purchase_key
        , o.time_order_received_utc
        , CAST(JSON_VALUE(item, '$.item_count') AS INT64) AS item_count
        , JSON_VALUE(item, '$.item_key') AS item_key
    FROM base_orders o,
    UNNEST(JSON_QUERY_ARRAY(o.item_basket_description)) AS item
),

basket_aggregates AS (

    SELECT
        purchase_key

        -- total quantity of items
        , SUM(item_count) AS total_items_in_basket

        -- comma-separated list of item_keys
        , STRING_AGG(DISTINCT item_key, ',') AS items_in_basket
    FROM basket_items
    GROUP BY purchase_key
),

promo_flag AS (

    SELECT DISTINCT
        b.purchase_key
        , TRUE AS is_promo_order
    FROM basket_items b
    JOIN {{ ref('dim_promo') }} p
      ON b.item_key = p.item_key
     AND b.time_order_received_utc >= p.promo_start_utc
     AND b.time_order_received_utc <  p.promo_end_utc
)

SELECT
    o.purchase_key
    , o.time_order_received_utc
    , o.time_order_received_date
    , o.customer_key
    , c.is_returning_customer
    , o.delivery_distance_meters
    , o.wolt_service_fee
    , o.courier_base_fee
    , o.total_basket_value

    -- derived basket metrics
    , a.total_items_in_basket
    , a.items_in_basket

    -- promo indicator
    , COALESCE(p.is_promo_order, FALSE) AS is_promo_order

FROM base_orders o
LEFT JOIN basket_aggregates a
  ON o.purchase_key = a.purchase_key
LEFT JOIN promo_flag p
  ON o.purchase_key = p.purchase_key
LEFT JOIN {{ ref('dim_customer') }} c
  ON o.customer_key = c.customer_key

{% if is_incremental() %}
WHERE o.time_order_received_utc >
      (SELECT MAX(time_order_received_utc) FROM {{ this }})
{% endif %}
