{{ config(materialized='view') }}

SELECT
    CAST(REPLACE(time_order_received_utc, ' Z', ' UTC') AS TIMESTAMP) AS time_order_received_utc
    , purchase_key
    , customer_key
    , delivery_distance_line_meters
    , wolt_service_fee
    , courier_base_fee
    , total_basket_value
    , item_basket_description
from {{ source('wolt_raw', 'purchase_logs_raw') }}
