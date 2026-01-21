{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'category_date_key',
    on_schema_change = 'sync_all_columns',
    tags = ['reporting', 'category', 'performance']
  )
}}

WITH base AS (

    SELECT
        DATE(p.time_order_received_utc) AS order_date
        , i.category
        , i.item_key
        , fi.quantity
        , fi.item_price_at_purchase
        , p.purchase_key
        , p.total_basket_value
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
        , category

        -- core performance metrics
        , SUM(fi.quantity * fi.item_price_at_purchase) AS revenue
        , SUM(fi.quantity) AS units_sold
        , COUNT(DISTINCT fi.item_key) AS distinct_items_sold
        , COUNT(DISTINCT p.purchase_key) AS orders

        -- promotion metrics
        , SUM(CASE WHEN p.is_promo_order THEN fi.quantity ELSE 0 END) AS promo_units_sold
        , COUNT(DISTINCT CASE WHEN p.is_promo_order THEN p.purchase_key END) AS promo_orders
    FROM base fi
    JOIN {{ ref('fct_purchases') }} p
      ON fi.purchase_key = p.purchase_key
    GROUP BY 1, 2
),

final AS (

    SELECT
        CONCAT(category, '_', FORMAT_DATE('%Y%m%d', order_date)) AS category_date_key
        , order_date
        , category

        , ROUND(revenue, 2) AS revenue
        , units_sold
        , orders
        , distinct_items_sold

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
