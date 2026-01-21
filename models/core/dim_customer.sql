{{ 
    config(
      materialized='table',
      tags=['core', 'customers']
    ) 
}}

WITH base AS (
    SELECT
        customer_key
        , time_order_received_utc
    FROM {{ ref('stg_purchase_logs') }}
),

aggregated AS (
    SELECT
        customer_key
        , MIN(time_order_received_utc) as first_purchase_date
        , MAX(time_order_received_utc) as last_purchase_date
        , COUNT(*) as total_purchases
        , CASE WHEN COUNT(*) > 1 
               THEN TRUE
               ELSE FALSE
          END AS is_returning_customer
    FROM base
    GROUP BY customer_key
)

SELECT *
FROM aggregated
