-- ============================================================
-- 04_validation_queries.sql
-- Retail Project - Validation and Sanity Checks
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PROJECT_WH;
USE DATABASE RETAIL_PROJECT_DB;


-- ============================================================
-- SECTION 1: VALIDATE CURATED ICEBERG TABLES
-- ============================================================

USE SCHEMA RETAIL_PROJECT_DB.CURATED;

-- Confirm the Iceberg tables exist.
SHOW ICEBERG TABLES
IN SCHEMA RETAIL_PROJECT_DB.CURATED;


-- Compare row counts across curated tables.
SELECT 'CUSTOMERS_CURATED' AS table_name, COUNT(*) AS row_count
FROM RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED

UNION ALL

SELECT 'ORDERS_CURATED', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.ORDERS_CURATED

UNION ALL

SELECT 'PRODUCTS_CURATED', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED

UNION ALL

SELECT 'SALES_CURATED', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED

UNION ALL

SELECT 'CATEGORY_SUMMARY', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.CATEGORY_SUMMARY

UNION ALL

SELECT 'PAYMENT_SUMMARY', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.PAYMENT_SUMMARY

UNION ALL

SELECT 'MARGIN', COUNT(*)
FROM RETAIL_PROJECT_DB.CURATED.MARGIN;


-- Check for duplicate business keys.
SELECT customer_id, COUNT(*) AS duplicate_count
FROM RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED
GROUP BY customer_id
HAVING COUNT(*) > 1;


SELECT product_id, COUNT(*) AS duplicate_count
FROM RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED
GROUP BY product_id
HAVING COUNT(*) > 1;


SELECT order_id, COUNT(*) AS duplicate_count
FROM RETAIL_PROJECT_DB.CURATED.ORDERS_CURATED
GROUP BY order_id
HAVING COUNT(*) > 1;


-- Check for null keys.
SELECT
    COUNT_IF(customer_id IS NULL) AS null_customer_ids
FROM RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED;

SELECT
    COUNT_IF(product_id IS NULL) AS null_product_ids
FROM RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED;

SELECT
    COUNT_IF(order_id IS NULL) AS null_order_ids,
    COUNT_IF(customer_id IS NULL) AS null_customer_ids,
    COUNT_IF(product_id IS NULL) AS null_product_ids
FROM RETAIL_PROJECT_DB.CURATED.ORDERS_CURATED;


-- ============================================================
-- SECTION 2: VALIDATE GOLD STAR SCHEMA
-- ============================================================

USE SCHEMA RETAIL_PROJECT_DB.GOLD;

SHOW TABLES
IN SCHEMA RETAIL_PROJECT_DB.GOLD;

SHOW VIEWS
IN SCHEMA RETAIL_PROJECT_DB.GOLD;


-- Gold table row counts.
SELECT 'DIM_CUSTOMER' AS table_name, COUNT(*) AS row_count
FROM RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER

UNION ALL

SELECT 'DIM_PRODUCT', COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT

UNION ALL

SELECT 'DIM_DATE', COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.DIM_DATE

UNION ALL

SELECT 'FACT_SALES', COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES;


-- Validate fact-to-dimension relationships.
SELECT
    COUNT_IF(customer_key IS NULL) AS unmatched_customers,
    COUNT_IF(product_key IS NULL) AS unmatched_products,
    COUNT_IF(date_key IS NULL) AS unmatched_dates
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES;


-- Confirm surrogate keys are unique.
SELECT customer_key, COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER
GROUP BY customer_key
HAVING COUNT(*) > 1;


SELECT product_key, COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT
GROUP BY product_key
HAVING COUNT(*) > 1;


SELECT date_key, COUNT(*)
FROM RETAIL_PROJECT_DB.GOLD.DIM_DATE
GROUP BY date_key
HAVING COUNT(*) > 1;


-- ============================================================
-- SECTION 3: BUSINESS SANITY CHECKS
-- ============================================================

-- Overall KPIs.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_EXECUTIVE_KPIS;


-- Daily sales trend.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_DAILY_SALES
ORDER BY full_date;


-- Highest revenue categories.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_CATEGORY_PERFORMANCE
ORDER BY revenue DESC;


-- Highest revenue products.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_PRODUCT_PERFORMANCE
ORDER BY revenue DESC;


-- Payment method performance.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_PAYMENT_PERFORMANCE
ORDER BY revenue DESC;


-- Top customers.
SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_CUSTOMER_PERFORMANCE
ORDER BY total_spent DESC;


-- ============================================================
-- SECTION 4: RECONCILIATION CHECKS
-- ============================================================

-- Revenue from curated data should match Gold fact table revenue.
SELECT
    ROUND(SUM(total_amount), 2) AS curated_revenue
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED;


SELECT
    ROUND(SUM(total_amount), 2) AS gold_revenue
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES;


-- Fact row count should normally match SALES_CURATED.
SELECT
    (SELECT COUNT(*)
     FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED) AS curated_sales_rows,

    (SELECT COUNT(*)
     FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES) AS fact_sales_rows;

-- ============================================================
-- DATA PREVIEW
-- ============================================================

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER
LIMIT 10;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT
LIMIT 10;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.DIM_DATE
LIMIT 10;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES
LIMIT 10;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_EXECUTIVE_KPIS;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_CATEGORY_PERFORMANCE;

SELECT *
FROM RETAIL_PROJECT_DB.GOLD.VW_PRODUCT_PERFORMANCE;
-- ============================================================
-- OPTIONAL: SUSPEND WAREHOUSE AFTER TESTING
-- ============================================================

ALTER WAREHOUSE PROJECT_WH SUSPEND;

SELECT
    customer_id,
    first_name,
    last_name
FROM RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED
ORDER BY customer_id;

SELECT
    product_id,
    product_name,
    category
FROM RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED
ORDER BY product_id;

SELECT DISTINCT s.product_id
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED s
LEFT JOIN RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED p
    ON s.product_id = p.product_id
WHERE p.product_id IS NULL
ORDER BY s.product_id;

SELECT DISTINCT s.product_id
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED s
LEFT JOIN RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED p
    ON s.product_id = p.product_id
WHERE p.product_id IS NULL
ORDER BY s.product_id;

SELECT DISTINCT s.customer_id
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED s
LEFT JOIN RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED c
    ON s.customer_id = c.customer_id
WHERE c.customer_id IS NULL
ORDER BY s.customer_id;

SELECT
    COUNT(*) AS total_fact_rows,
    COUNT_IF(product_key IS NULL) AS null_product_keys,
    COUNT_IF(customer_key IS NULL) AS null_customer_keys
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES;

SELECT
    p.category,
    ROUND(SUM(f.total_amount), 2) AS revenue
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
JOIN RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT p
    ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY revenue DESC;

SELECT DISTINCT f.product_key
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
LEFT JOIN RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT p
    ON f.product_key = p.product_key
WHERE p.product_key IS NULL;