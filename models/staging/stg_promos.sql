{{ config(materialized='view') }}

SELECT
    promo_start_date
    , promo_end_date
    , item_key
    , promo_type
    , discount_in_percentage
FROM {{ source('wolt_raw', 'promos_raw') }}
