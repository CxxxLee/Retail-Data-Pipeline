SNOWFLAKE
-- ============================================================
-- Retail Project - Gold Dimensional Model and Reporting Views
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE PROJECT_WH;
USE DATABASE RETAIL_PROJECT_DB;

-- ============================================================
-- SECTION 1: CREATE GOLD SCHEMA
-- ============================================================

CREATE SCHEMA IF NOT EXISTS RETAIL_PROJECT_DB.GOLD;
USE SCHEMA RETAIL_PROJECT_DB.GOLD;


-- ============================================================
-- SECTION 2: CREATE DIMENSION TABLES
-- ============================================================

-- Customer dimension
-- ROW_NUMBER creates a simple surrogate key for Power BI relationships.
CREATE OR REPLACE TABLE RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    signup_date,
    country,
    state,
    postal_code,
    is_active,
    loyalty_points
FROM RETAIL_PROJECT_DB.CURATED.CUSTOMERS_CURATED;


-- Product dimension
CREATE OR REPLACE TABLE RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
    product_id,
    product_name,
    category,
    brand,
    price,
    cost,
    stock_quantity,
    weight_kg,
    created_date,
    is_active
FROM RETAIL_PROJECT_DB.CURATED.PRODUCTS_CURATED;


-- Date dimension
-- Uses the order dates that currently exist in the sales data.
CREATE OR REPLACE TABLE RETAIL_PROJECT_DB.GOLD.DIM_DATE AS
SELECT DISTINCT
    TO_NUMBER(TO_CHAR(order_date, 'YYYYMMDD')) AS date_key,
    order_date AS full_date,
    YEAR(order_date) AS year,
    QUARTER(order_date) AS quarter,
    MONTH(order_date) AS month_number,
    MONTHNAME(order_date) AS month_name,
    DAY(order_date) AS day_of_month,
    DAYOFWEEK(order_date) AS day_of_week_number,
    DAYNAME(order_date) AS day_name
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED
WHERE order_date IS NOT NULL;


-- ============================================================
-- SECTION 3: CREATE FACT TABLE
-- ============================================================

-- Sales fact table
-- Stores measurable sales events and foreign keys to the dimensions.
CREATE OR REPLACE TABLE RETAIL_PROJECT_DB.GOLD.FACT_SALES AS
SELECT
    s.order_id,
    c.customer_key,
    p.product_key,
    TO_NUMBER(TO_CHAR(s.order_date, 'YYYYMMDD')) AS date_key,
    s.customer_id,
    s.product_id,
    s.order_date,
    s.quantity,
    s.unit_price,
    s.discount_pct,
    s.total_amount,
    s.profit,
    s.payment_method,
    s.order_status
FROM RETAIL_PROJECT_DB.CURATED.SALES_CURATED s
LEFT JOIN RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER c
    ON s.customer_id = c.customer_id
LEFT JOIN RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT p
    ON s.product_id = p.product_id;


-- ============================================================
-- SECTION 4: CREATE BUSINESS-READY REPORTING VIEWS
-- ============================================================

-- Executive KPI view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_EXECUTIVE_KPIS AS
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(
        SUM(profit) / NULLIF(SUM(total_amount), 0) * 100,
        2
    ) AS overall_profit_margin_pct,
    ROUND(
        SUM(total_amount) / NULLIF(COUNT(DISTINCT order_id), 0),
        2
    ) AS average_order_value
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES;


-- Daily sales trend view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_DAILY_SALES AS
SELECT
    d.full_date,
    d.year,
    d.quarter,
    d.month_number,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
JOIN RETAIL_PROJECT_DB.GOLD.DIM_DATE d
    ON f.date_key = d.date_key
GROUP BY
    d.full_date,
    d.year,
    d.quarter,
    d.month_number,
    d.month_name;


-- Category performance view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_CATEGORY_PERFORMANCE AS
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND(
        SUM(f.profit) / NULLIF(SUM(f.total_amount), 0) * 100,
        2
    ) AS profit_margin_pct
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
JOIN RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT p
    ON f.product_key = p.product_key
GROUP BY p.category;


-- Product performance view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_PRODUCT_PERFORMANCE AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.total_amount), 2) AS revenue,
    ROUND(SUM(f.profit), 2) AS profit,
    ROUND(
        SUM(f.profit) / NULLIF(SUM(f.total_amount), 0) * 100,
        2
    ) AS profit_margin_pct
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
JOIN RETAIL_PROJECT_DB.GOLD.DIM_PRODUCT p
    ON f.product_key = p.product_key
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.brand;


-- Payment method performance view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_PAYMENT_PERFORMANCE AS
SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS units_sold,
    ROUND(SUM(total_amount), 2) AS revenue,
    ROUND(SUM(profit), 2) AS profit
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES
GROUP BY payment_method;


-- Customer performance view
CREATE OR REPLACE VIEW RETAIL_PROJECT_DB.GOLD.VW_CUSTOMER_PERFORMANCE AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.state,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS units_purchased,
    ROUND(SUM(f.total_amount), 2) AS total_spent,
    ROUND(SUM(f.profit), 2) AS total_profit_generated
FROM RETAIL_PROJECT_DB.GOLD.FACT_SALES f
JOIN RETAIL_PROJECT_DB.GOLD.DIM_CUSTOMER c
    ON f.customer_key = c.customer_key
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name,
    c.country,
    c.state;


-- Preview business-ready views
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_EXECUTIVE_KPIS;
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_DAILY_SALES ORDER BY full_date;
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_CATEGORY_PERFORMANCE ORDER BY revenue DESC;
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_PRODUCT_PERFORMANCE ORDER BY revenue DESC;
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_PAYMENT_PERFORMANCE ORDER BY revenue DESC;
SELECT * FROM RETAIL_PROJECT_DB.GOLD.VW_CUSTOMER_PERFORMANCE ORDER BY total_spent DESC;


-- Optional: suspend the warehouse when finished testing.
ALTER WAREHOUSE PROJECT_WH SUSPEND;