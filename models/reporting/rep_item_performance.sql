{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'item_date_key',
    on_schema_change = 'sync_all_columns',
    tags = ['reporting', 'product', 'performance']
  )
}}

WITH base AS (

  SELECT
      DATE(p.time_order_received_utc) AS order_date
      , i.item_key
      , i.item_name_en
      , i.category
      , fi.quantity
      , fi.item_price_at_purchase
      , p.purchase_key
      , p.is_promo_order
  FROM {{ ref('fct_purchases_items') }} fi
  JOIN {{ ref('fct_purchases') }} p
    ON fi.purchase_key = p.purchase_key
  JOIN {{ ref('dim_item') }} i
    ON fi.item_key = i.item_key
),

aggregated AS (

  SELECT
      order_date
      , item_key
      , item_name_en
      , category

      -- core metrics
      , SUM(quantity * item_price_at_purchase) AS revenue
      , SUM(quantity) AS units_sold
      , COUNT(DISTINCT purchase_key) AS orders

      -- promo metrics
      , SUM(CASE WHEN is_promo_order THEN quantity ELSE 0 END) AS promo_units_sold
      , COUNT(DISTINCT CASE WHEN is_promo_order THEN purchase_key END) AS promo_orders
  FROM base
  GROUP BY 1,2,3,4
),

final AS (

  SELECT
      CONCAT(item_key, '_', FORMAT_DATE('%Y%m%d', order_date)) AS item_date_key
      , order_date
      , item_key
      , item_name_en
      , category

      , ROUND(revenue, 2) AS revenue
      , units_sold
      , orders

      , promo_units_sold
      , promo_orders

      -- derived KPIs
      , ROUND(SAFE_DIVIDE(revenue, units_sold), 3) AS avg_unit_price
      , ROUND(SAFE_DIVIDE(promo_units_sold, units_sold), 3) AS promo_unit_share
      , ROUND(SAFE_DIVIDE(promo_orders, orders), 3) AS promo_order_share
  FROM aggregated
)

SELECT *
FROM final

{% if is_incremental() %}
WHERE order_date >
  (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
