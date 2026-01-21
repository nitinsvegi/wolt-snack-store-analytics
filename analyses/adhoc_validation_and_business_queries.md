# Ad-hoc Validation & Business Analysis Queries

This document contains exploratory and validation queries executed in BigQuery
to validate source data, confirm modeling assumptions, and demonstrate how the
final models support business questions. These queries are not part of the dbt
transformation pipeline and are provided for transparency and review.

-- Check raw sources 

--1. item_logs_raw source
SELECT * 
FROM wolt_store_snack_raw.item_logs_raw;
-- 648 rows

SELECT log_item_id, COUNT(*) AS COUNT
FROM wolt_store_snack_raw.item_logs_raw
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 177 duplicate records 

SELECT * 
FROM wolt_store_snack_raw.item_logs_raw
WHERE TRUE
  AND log_item_id IN ('b4fa9b037afadefa55adfeff4989e02f', '0c1059b35886a3e540403984ad44bb96', '8726ef6f3f7ff6c035c331826f4211ed','6f9184487f5a063c0c77d7d3076c52eb')
ORDER BY log_item_id;
-- All the duplicate items here having a difference in product_base_price

SELECT * 
FROM wolt_store_snack_raw.item_logs_raw
WHERE TRUE
  AND log_item_id IN ('0bbf56269400abfba1c6c3de290a3d7c', 'a4674513975e6a4ef80cab8ffe44a72f', 'b67d4d99861bd2e1c2746632ad2bfa9d','8975c3331c423b65dd3159649841e2f6')
ORDER BY log_item_id;
-- The seconf set of duplicate items taken for sanitu check are having a difference in product_base_price

--2. promos_raw source
SELECT * 
FROM wolt_store_snack_raw.promos_raw;
--112 records

--3. purchase_logs_raw source
SELECT * 
FROM wolt_store_snack_raw.purchase_logs_raw;
--98871 records

SELECT purchase_key, COUNT(*) AS COUNT
FROM wolt_store_snack_raw.purchase_logs_raw
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 0 duplicate records 


-----------------------------------------------------
-- Check staging sources 

--1. item_logs_raw source
SELECT * 
FROM wolt_store_snack_staging.stg_item_logs;
-- 648 rows

SELECT log_item_id, COUNT(*) AS COUNT
FROM wolt_store_snack_staging.stg_item_logs
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 177 duplicate records 

--2. promos_raw source
SELECT * 
FROM wolt_store_snack_staging.stg_promos;
--112 records

--3. purchase_logs_raw source
SELECT * 
FROM wolt_store_snack_staging.stg_purchase_logs;
--98871 records

SELECT purchase_key, COUNT(*) AS COUNT
FROM wolt_store_snack_staging.stg_purchase_logs
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 0 duplicate records 

-----------------------------------------------------
-- Check dimensional sources 

--1.dim_customer
SELECT * 
FROM wolt_store_snack_analytics.dim_customer;
--2001 Cusomters

SELECT 
  is_returning_customer
  , COUNT(*) AS num_customers
  , AVG(total_purchases) AS avg_purchases
FROM wolt_store_snack_analytics.dim_customer
GROUP BY 1;
-- All customers seem to be returning customers with avergae number of purchases to be ~49 


SELECT 
  SUM(CASE WHEN is_returning_customer THEN 1 ELSE 0 END) AS returning_customers
  , COUNT(*) AS total_customers
  , SAFE_DIVIDE(SUM(CASE WHEN is_returning_customer THEN 1 ELSE 0 END),COUNT(*)) AS returning_customer_rate
FROM wolt_store_snack_analytics.dim_customer;
-- Returning customer rate is 100%

SELECT 
  DATE_TRUNC(first_purchase_date, MONTH) AS first_purchase_month
  , COUNT(*) AS new_customers
FROM wolt_store_snack_analytics.dim_customer
GROUP BY 1
ORDER BY 1;
-- There are were customers who started using the Wolt Snack Store in Jan 2023 and then there was a decline with the number of customer as month progressed



--2.dim_item_history
SELECT * 
FROM wolt_store_snack_analytics.dim_item_history;
--648 row

SELECT * 
FROM wolt_store_snack_analytics.dim_item_history
WHERE price_including_vat IS NULL;
-- 108 records do not have NULL product_base_price and 540 rows have a valid product_base_price

SELECT COUNT(DISTINCT item_key)
FROM wolt_store_snack_analytics.dim_item_history;
-- 60 unique item_key meaning there are 60 products

SELECT COUNT(DISTINCT log_item_id)
FROM wolt_store_snack_analytics.dim_item_history;
-- 471 unique log_item_id ~ Many events per item

SELECT DISTINCT(vat_percentage)
FROM wolt_store_snack_analytics.dim_item_history;
-- 19

SELECT * 
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND log_item_id = 'b6668ac8dec2b7fef58620816d6a759c'
  AND item_key = '971c3d87aff5615604a3d1ef052d8cd9'
  AND valid_from_utc = '2023-01-23 12:43:40.317000 UTC';

SELECT * 
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND item_key = '971c3d87aff5615604a3d1ef052d8cd9';


SELECT * 
FROM wolt_store_snack_staging.stg_item_logs
WHERE TRUE
  AND log_item_id = 'b6668ac8dec2b7fef58620816d6a759c'
  AND item_key = '971c3d87aff5615604a3d1ef052d8cd9';


SELECT CONCAT(
  item_key, 
  '_', 
  FORMAT_TIMESTAMP('%Y%m%d%H%M%S', valid_from_utc),
  '_',
  COALESCE(CAST(price_including_vat AS STRING), 'missing')
) AS item_history_key
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- this provides a unique primary key


SELECT 
  price_type
  , COUNT(*) AS count_price_types
FROM wolt_store_snack_analytics.dim_item_history
GROUP BY 1;
-- positive	471	
-- missing	108
-- negative	69	


SELECT DISTINCT(item_key)
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND brand_name IS NULL;
-- 4 items have no brand name


--3.dim_item
-- Validation checks with dim_item_history first before creating the table
WITH base AS (
SELECT item_key
  ,  ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND valid_to_utc IS NULL     -- only the current version
)
SELECT item_key
  , COUNT(*) AS COUNT
FROM base
WHERE TRUE
  AND rn = 1
GROUP BY 1
HAVING COUNT > 1;
-- No Duplicate items

WITH base AS (
SELECT item_key
  ,  ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND valid_to_utc IS NULL     -- only the current version
)
SELECT COUNT(item_key)
FROM base
WHERE TRUE
  AND rn = 1;
--Getting 60 items, which checks out 


WITH base AS (
SELECT item_key
  , price_type
  , ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND valid_to_utc IS NULL     -- only the current version
)
SELECT price_type
  , COUNT(*) COUNT
FROM base
WHERE TRUE
  AND rn = 1
GROUP BY 1;
-- We notice both negative & missing values when we look at some of the latest prices

SELECT item_key
  , price_type
  , price_including_vat
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND item_key IN ('564b722b1d92225859390ea030ddd983', '5d3969ae02b82a2e1f84457d03fedcab')
  AND valid_to_utc IS NULL     -- only the current version
ORDER BY item_key;
-- Examples of negative & missing prices 

SELECT *
FROM wolt_store_snack_analytics.dim_item_history
WHERE TRUE
  AND item_key IN ('564b722b1d92225859390ea030ddd983', '5d3969ae02b82a2e1f84457d03fedcab')
ORDER BY item_key, valid_from_utc ASC;


WITH latest_valid_price AS (
    -- Pick the most recent *valid positive* price per item
    SELECT
        item_key
        , price_including_vat AS current_price_including_vat
        , valid_from_utc AS price_effective_from_utc
        , ROW_NUMBER() OVER (PARTITION BY item_key ORDER BY valid_from_utc DESC) AS rn
    FROM wolt_store_snack_analytics.dim_item_history
    WHERE price_including_vat > 0
)
SELECT * FROM latest_valid_price
WHERE rn = 1;
-- Taking only valid postive prices for items, the query confims we have positive (valid) prices for all 60 items


SELECT *
FROM wolt_store_snack_analytics.dim_item;
-- 60 Records

SELECT COUNT(DISTINCT item_key)
FROM wolt_store_snack_analytics.dim_item;
-- 60 unique item_key meaning there are 60 products

SELECT item_key, COUNT(*) AS COUNT
FROM wolt_store_snack_analytics.dim_item
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 0 duplicate records 


SELECT *
FROM wolt_store_snack_analytics.dim_item
WHERE TRUE
  AND current_price_including_vat > 0;
-- Returned 60 reocords, indication all items has postive price values


--4.dim_promo
SELECT *
FROM wolt_store_snack_analytics.dim_promo;
-- 112 Records

SELECT COUNT(DISTINCT promo_key)
FROM wolt_store_snack_analytics.dim_promo;
-- 112 Promotions

SELECT promo_key, COUNT(*) AS COUNT
FROM wolt_store_snack_analytics.dim_promo
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- 0 duplicate records 

--5. fct_purchases
SELECT *
FROM wolt_store_snack_analytics.fct_purchases;
-- 98871 Records

SELECT COUNT(DISTINCT purchase_key)
FROM wolt_store_snack_analytics.fct_purchases;
-- 98871 Purchase Orders

SELECT purchase_key, COUNT(*) AS COUNT
FROM wolt_store_snack_analytics.fct_purchases
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- No Duplicates

SELECT COUNT(*) AS invalid_customer_rows
FROM wolt_store_snack_analytics.fct_purchases f
LEFT JOIN wolt_store_snack_analytics.dim_customer d
  ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL;
-- O invalid customers

-- Check if all the items are valid items in the basket
WITH all_items AS (
  SELECT p.purchase_key
      , TRIM(item_key) AS item_key
  FROM wolt_store_snack_analytics.fct_purchases p,
  UNNEST(SPLIT(p.items_in_basket, ',')) AS item_key
)
SELECT
  COUNT(*) AS invalid_item_rows
FROM all_items e
LEFT JOIN wolt_store_snack_analytics.dim_item d
  ON e.item_key = d.item_key
WHERE d.item_key IS NULL;
-- O invalid items

SELECT is_promo_order
  , COUNT(*) AS order_count
FROM wolt_store_snack_analytics.fct_purchases
GROUP BY is_promo_order;
--false	92844 & true	6027

SELECT COUNT(DISTINCT customer_key) AS returning_customers
FROM wolt_store_snack_analytics.fct_purchases
WHERE is_returning_customer = TRUE;
--All 2001 are repeat customers


SELECT COUNT(DISTINCT customer_key) AS customers_with_promo_orders
FROM wolt_store_snack_analytics.fct_purchases
WHERE is_promo_order = TRUE;
-- Only 1689 customer out of 2001 customers bought items on promotions


--6. fct_purchases_items
SELECT *
FROM wolt_store_snack_analytics.fct_purchases_items;
-- 45684 items purchase item keys

SELECT COUNT(DISTINCT purchase_item_key)
FROM wolt_store_snack_analytics.fct_purchases_items;
-- 45684 dinstinct items purchase item keys

SELECT COUNT(DISTINCT item_key)
FROM wolt_store_snack_analytics.fct_purchases_items;
--41 dinstinct items

SELECT purchase_item_key, COUNT(*) AS COUNT
FROM wolt_store_snack_analytics.fct_purchases_items
WHERE TRUE
GROUP BY 1
HAVING COUNT > 1;
-- No Duplicates

SELECT
  COUNT(*) AS invalid_item_rows
FROM wolt_store_snack_analytics.fct_purchases_items e
LEFT JOIN wolt_store_snack_analytics.dim_item d
  ON e.item_key = d.item_key
WHERE d.item_key IS NULL;
-- O invalid items


WITH item_totals AS (
  SELECT purchase_key
      , SUM(item_price_at_purchase * quantity) AS gross_item_value
  FROM wolt_store_snack_analytics.fct_purchases_items
  GROUP BY purchase_key
)

SELECT COUNT(*) AS mismatched_non_promo_orders
FROM wolt_store_snack_analytics.fct_purchases p
JOIN item_totals i
  ON p.purchase_key = i.purchase_key
WHERE p.is_promo_order = FALSE
  AND ABS(i.gross_item_value - p.total_basket_value) > 0.05;

-- 0 mismatched non promo orders when total basket value from fct_purchases is compared agasint positive price for items in fct_purchases_items



-- Example querires for a purchase_key from differnet tables to ensure total basket value matches the positive price for an item in dim_item_histoey
SELECT *
FROM wolt_store_snack_staging.stg_purchase_logs
WHERE TRUE
  AND purchase_key = 'cbf99a8e70598fd3437d0206e1676ac7';
/*

[
  {
    "item_count": 2,
    "item_key": "7aef490acb1ca55f113afe02977b9e8f"
  },
  {
    "item_count": 2,
    "item_key": "9f8bd236cf455c9fa6a5fb11146b9368"
  },
  {
    "item_count": 2,
    "item_key": "e98605036c98cd2c57d3eaf01e1c49ad"
  },
  {
    "item_count": 2,
    "item_key": "92d78ca20860dd50a1748fe2b0c8dc78"
  }
]
*/

SELECT *
FROM wolt_store_snack_analytics.fct_purchases
WHERE TRUE
  AND purchase_key = 'cbf99a8e70598fd3437d0206e1676ac7';
/*
7aef490acb1ca55f113afe02977b9e8f,9f8bd236cf455c9fa6a5fb11146b9368,e98605036c98cd2c57d3eaf01e1c49ad,92d78ca20860dd50a1748fe2b0c8dc78
total_basket_value = 19.9

*/

SELECT *
FROM wolt_store_snack_analytics.fct_purchases_items
WHERE TRUE
  AND purchase_key = 'cbf99a8e70598fd3437d0206e1676ac7';

-- (1,68*2) + (3,48*2) + (2.56*2) + (2,23*2) = 19.9


-- For Discounted items 
WITH item_with_discount AS (

  SELECT
      i.purchase_key
      , i.item_key
      , i.quantity
      , i.item_price_at_purchase AS list_price

      -- promo info
      , COALESCE(p.discount_in_percentage, 0) AS discount_pct

      -- discounted unit price
      , i.item_price_at_purchase * (1 - COALESCE(p.discount_in_percentage, 0) / 100.0) AS discounted_unit_price
  FROM wolt_store_snack_analytics.fct_purchases_items i
  LEFT JOIN wolt_store_snack_analytics.dim_promo p
    ON i.item_key = p.item_key
   AND i.time_order_received_utc >= p.promo_start_utc
   AND i.time_order_received_utc <  p.promo_end_utc
),

basket_totals AS (

  SELECT
      purchase_key
      -- gross value before discount
      , SUM(list_price * quantity) AS gross_basket_value
      -- net value after applying discount %
      , SUM(discounted_unit_price * quantity) AS discounted_basket_value
  FROM item_with_discount
  GROUP BY purchase_key
)

SELECT
    p.purchase_key
    , p.total_basket_value AS recorded_basket_value
    , b.discounted_basket_value
    , b.gross_basket_value
    -- difference after discounts
    , ROUND(b.discounted_basket_value - p.total_basket_value, 2) AS value_difference
FROM wolt_store_snack_analytics.fct_purchases p
JOIN basket_totals b
  ON p.purchase_key = b.purchase_key
WHERE ABS(b.discounted_basket_value - p.total_basket_value) > 0.05
ORDER BY ABS(value_difference) DESC;

-- Promotions are applied deterministically and there zoro records that have a mismatch indicating promo orders also match perfectly after applying the percentage



---------------------------------------------------------------------------------------------------------------------------------------------------------------
--1) What area is the store serving in any given period?
SELECT
  DATE(time_order_received_utc) AS order_date
  , COUNT(*) AS orders
  , AVG(delivery_distance_meters) AS avg_delivery_distance_m
  , MAX(delivery_distance_meters) AS max_delivery_distance_m
FROM wolt_store_snack_analytics.fct_purchases
GROUP BY order_date
ORDER BY order_date;

--2) What items are being bought and what price are they going for in any given period?
SELECT
  DATE(p.time_order_received_utc) AS order_date
  , i.item_key
  , d.item_name_en
  , AVG(i.item_price_at_purchase) AS avg_price
  , SUM(i.quantity) AS units_sold
FROM wolt_store_snack_analytics.fct_purchases_items i
JOIN wolt_store_snack_analytics.fct_purchases p
  ON i.purchase_key = p.purchase_key
JOIN wolt_store_snack_analytics.dim_item d
  ON i.item_key = d.item_key
GROUP BY order_date, i.item_key, d.item_name_en
ORDER BY order_date, units_sold DESC;

-- 3) How many items are being bought on promotion in any given period?
SELECT
  DATE(p.time_order_received_utc) AS order_date
  , SUM(i.quantity) AS promo_items_sold
FROM wolt_store_snack_analytics.fct_purchases_items i
JOIN wolt_store_snack_analytics.fct_purchases p
  ON i.purchase_key = p.purchase_key
WHERE i.was_on_promo = TRUE
GROUP BY order_date
ORDER BY order_date;

--4) Are customers taking advantage of promotions?
SELECT
  COUNT(DISTINCT customer_key) AS total_customers
  , COUNT(DISTINCT IF(is_promo_order, customer_key, NULL)) AS promo_customers
  , ROUND(COUNT(DISTINCT IF(is_promo_order, customer_key, NULL)) 
        /COUNT(DISTINCT customer_key), 2) AS pct_customers_using_promos
FROM wolt_store_snack_analytics.fct_purchases;

--5) Are customers coming back to the store?
SELECT
  COUNT(DISTINCT customer_key) AS total_customers
  , COUNT(DISTINCT IF(is_returning_customer, customer_key, NULL)) AS returning_customers
FROM wolt_store_snack_analytics.fct_purchases;

--6) How do Wolt and Courier fees compare to basket value?
SELECT
  DATE(time_order_received_utc) AS order_date
  , AVG(wolt_service_fee) AS avg_wolt_fee
  , AVG(courier_base_fee) AS avg_courier_fee
  , AVG(total_basket_value) AS avg_basket_value
  , ROUND(AVG(wolt_service_fee + courier_base_fee)
        / AVG(total_basket_value), 2) AS fee_to_basket_ratio
FROM wolt_store_snack_analytics.fct_purchases
GROUP BY order_date
ORDER BY order_date;

--7) How much revenue has the company generated in any given period?
SELECT
  FORMAT_DATE('%Y-%m', DATE(time_order_received_utc)) AS order_month
  , ROUND(SUM(total_basket_value), 2) AS total_revenue_in_euros -- Excludes service & courier fees, per definition
FROM wolt_store_snack_analytics.fct_purchases
GROUP BY order_month
ORDER BY order_month;

--8) How much are courier costs in any given period?
SELECT
  DATE(time_order_received_utc) AS order_date
  , ROUND(SUM(courier_base_fee), 2) AS total_courier_costs_in_euors
FROM wolt_store_snack_analytics.fct_purchases
GROUP BY order_date
ORDER BY order_date;

-----------------------------------------------------
--Reporting table : Category level Performance
--Category Ranking : Identify which categories drive the most revenue and volume overall.
SELECT
  category
  , ROUND(SUM(revenue), 2) AS total_revenue
  , SUM(units_sold) AS total_units_sold
  , SUM(orders) AS total_orders
FROM wolt_store_snack_reporting.rep_category_performance
GROUP BY category
ORDER BY total_revenue DESC;


--Promotion dependence by category: To explain why some categories perform better than others.

SELECT
  category
  , ROUND(SUM(revenue), 2) AS total_revenue
  , ROUND(AVG(promo_unit_share), 2) AS avg_promo_unit_share
  , ROUND(AVG(promo_order_share), 2) AS avg_promo_order_share
FROM wolt_store_snack_reporting.rep_category_performance
GROUP BY category
ORDER BY avg_promo_unit_share DESC;

-- Promo vs non-promo performance comparison : Show whether promotions are amplifying demand or subsidizing weak categories.
SELECT
  category
  , SUM(promo_units_sold) AS promo_units
  , SUM(units_sold) - SUM(promo_units_sold) AS non_promo_units
  , ROUND(SAFE_DIVIDE(SUM(promo_units_sold), SUM(units_sold)), 2) AS promo_unit_ratio
FROM wolt_store_snack_reporting.rep_category_performance
GROUP BY category
ORDER BY promo_unit_ratio DESC;

--Identify “star” vs “fragile” categories
SELECT
  category
  , ROUND(SUM(revenue), 2) AS total_revenue
  , ROUND(AVG(promo_unit_share), 2) AS promo_dependency
  , COUNT(DISTINCT order_date) AS active_days
FROM wolt_store_snack_reporting.rep_category_performance
GROUP BY category
ORDER BY total_revenue DESC;

--Trend Analysis: Comparing first half vs second half of the year to get a clear growth direction per category.

WITH monthly AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS month
    , category
    , SUM(revenue) AS monthly_revenue
  FROM wolt_store_snack_reporting.rep_category_performance
  GROUP BY month, category
),

periods AS (
  SELECT
    category
    , AVG(CASE WHEN month < '2023-07-01' THEN monthly_revenue END) AS avg_revenue_h1
    , AVG(CASE WHEN month >= '2023-07-01' THEN monthly_revenue END) AS avg_revenue_h2
  FROM monthly
  GROUP BY category
)
SELECT
  category
  , ROUND(avg_revenue_h1, 2) AS avg_monthly_revenue_h1
  , ROUND(avg_revenue_h2, 2) AS avg_monthly_revenue_h2
  , ROUND(avg_revenue_h2 - avg_revenue_h1, 2) AS absolute_growth --how much average monthly revenue increased
  , ROUND(SAFE_DIVIDE(avg_revenue_h2 - avg_revenue_h1, avg_revenue_h1), 2) AS growth_rate --relative growth (percentage-style signal) 
FROM periods
ORDER BY growth_rate DESC;


-- Momentum Analysis : Identify acceleration vs stagnation: Which categories are consistently gaining momentum month after month ?
WITH monthly AS (
  SELECT
    DATE_TRUNC(order_date, MONTH) AS month
    , category
    , SUM(revenue) AS monthly_revenue
  FROM wolt_store_snack_reporting.rep_category_performance
  GROUP BY month, category
),
with_lag AS (
  SELECT
    category
    , month
    , monthly_revenue
    , LAG(monthly_revenue) OVER (
        PARTITION BY category ORDER BY month
      ) AS prev_month_revenue
  FROM monthly
)
SELECT
  category
  , ROUND(AVG(monthly_revenue - prev_month_revenue), 2) AS avg_monthly_change
FROM with_lag
WHERE prev_month_revenue IS NOT NULL
GROUP BY category
ORDER BY avg_monthly_change DESC;

-- Seasonlity Signal: Collapse months into seasons
SELECT
  category
  , CASE
      WHEN EXTRACT(MONTH FROM order_date) IN (12,1,2) THEN 'Winter'
      WHEN EXTRACT(MONTH FROM order_date) IN (3,4,5) THEN 'Spring'
      WHEN EXTRACT(MONTH FROM order_date) IN (6,7,8) THEN 'Summer'
      ELSE 'Autumn'
    END AS season
  , ROUND(AVG(revenue), 2) AS avg_revenue -- average daily revenue per category by season
FROM wolt_store_snack_reporting.rep_category_performance
GROUP BY category, season
ORDER BY category, season;
-----------------------------------------------------
--Reporting table : Category level Performance

-- Star products (top revenue & volume)
SELECT
  category
  , item_name_en
  , ROUND(SUM(revenue), 2) AS total_revenue
  , SUM(units_sold) AS total_units
FROM wolt_store_snack_reporting.rep_item_performance
GROUP BY category, item_name_en
ORDER BY total_revenue DESC
LIMIT 20;

--Promotion dependence by product
SELECT
  item_name_en
  , category
  , ROUND(AVG(promo_unit_share), 2) AS promo_dependency
  , ROUND(SUM(revenue), 2) AS total_revenue
FROM wolt_store_snack_reporting.rep_item_performance
GROUP BY item_name_en, category
ORDER BY promo_dependency DESC;

--Price vs volume positioning
SELECT
  category
  , item_name_en
  , ROUND(AVG(avg_unit_price), 2) AS avg_price
  , SUM(units_sold) AS total_units
FROM wolt_store_snack_reporting.rep_item_performance
GROUP BY category, item_name_en
ORDER BY avg_price DESC;

--Co-purchase analysis
WITH pairs AS (
  SELECT
    a.item_key AS item_a_key
    , b.item_key AS item_b_key
    , COUNT(*) AS times_bought_together
  FROM wolt_store_snack_analytics.fct_purchases_items a
  JOIN wolt_store_snack_analytics.fct_purchases_items b
    ON a.purchase_key = b.purchase_key
   AND a.item_key < b.item_key     -- prevent double counting & self-pairs
  GROUP BY 1, 2
)

SELECT
  p.item_a_key
  , da.item_name_en AS item_a_name
  , da.category     AS item_a_category

  , p.item_b_key
  , db.item_name_en AS item_b_name
  , db.category     AS item_b_category

  , p.times_bought_together
FROM pairs p
JOIN wolt_store_snack_analytics.dim_item da
  ON p.item_a_key = da.item_key
JOIN wolt_store_snack_analytics.dim_item db
  ON p.item_b_key = db.item_key
ORDER BY p.times_bought_together DESC
LIMIT 20;