{{ config(materialized='view') }}

SELECT
    log_item_id
    , item_key
    , CAST(REPLACE(time_log_created_utc, ' Z', ' UTC') AS TIMESTAMP) AS time_log_created_utc
    , payload_json
FROM {{ source('wolt_raw', 'item_logs_raw') }}

