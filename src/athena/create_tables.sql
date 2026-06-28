-- ============================================================
-- Athena External Tables — Gold Layer
-- Run these DDLs once in the Athena console
-- Database: ecommerce_gold
-- ============================================================

-- 0. Create database (run first)
CREATE DATABASE IF NOT EXISTS ecommerce_gold
COMMENT 'E-Commerce Analytics Gold Layer';

CREATE DATABASE IF NOT EXISTS ecommerce_silver
COMMENT 'E-Commerce Analytics Silver Layer';


-- ============================================================
-- GOLD TABLES
-- ============================================================

-- 1. Daily Revenue Summary
DROP TABLE IF EXISTS ecommerce_gold.daily_revenue_summary;
CREATE EXTERNAL TABLE ecommerce_gold.daily_revenue_summary (
    channel           STRING,
    total_orders      BIGINT,
    gross_revenue     DOUBLE,
    promo_revenue     DOUBLE,
    avg_item_value    DOUBLE,
    total_items_sold  BIGINT,
    total_units       BIGINT,
    aov               DOUBLE,
    promo_pct         DOUBLE,
    updated_at        TIMESTAMP,
    date_partition    STRING
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-gold/gold/daily_revenue_summary/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- Refresh partitions
MSCK REPAIR TABLE ecommerce_gold.daily_revenue_summary;


-- 2. Customer RFM Segments
DROP TABLE IF EXISTS ecommerce_gold.customer_rfm;
CREATE EXTERNAL TABLE ecommerce_gold.customer_rfm (
    customer_id       STRING,
    last_order_date   DATE,
    frequency         BIGINT,
    monetary          DOUBLE,
    recency_days      INT,
    r_score           INT,
    f_score           INT,
    m_score           INT,
    rfm_score         INT,
    rfm_segment       STRING,
    country           STRING,
    segment           STRING,
    signup_date       DATE,
    updated_at        TIMESTAMP,
    date_partition    STRING
)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-gold/gold/customer_rfm/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');


-- 3. Product Performance
DROP TABLE IF EXISTS ecommerce_gold.product_performance;
CREATE EXTERNAL TABLE ecommerce_gold.product_performance (
    product_id        STRING,
    units_sold        BIGINT,
    revenue           DOUBLE,
    order_count       BIGINT,
    avg_discount      DOUBLE,
    last_sold_date    DATE,
    name              STRING,
    price             DOUBLE,
    cost              DOUBLE,
    margin_pct        DOUBLE,
    rating            DOUBLE,
    review_count      INT,
    is_low_stock      BOOLEAN,
    rank_by_revenue   INT,
    rank_by_units     INT,
    updated_at        TIMESTAMP,
    date_partition    STRING
)
PARTITIONED BY (category STRING)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-gold/gold/product_performance/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE ecommerce_gold.product_performance;


-- 4. Geographic Revenue
DROP TABLE IF EXISTS ecommerce_gold.geo_revenue;
CREATE EXTERNAL TABLE ecommerce_gold.geo_revenue (
    country           STRING,
    revenue           DOUBLE,
    orders            BIGINT,
    unique_customers  BIGINT,
    aov               DOUBLE,
    updated_at        TIMESTAMP,
    date_partition    STRING
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-gold/gold/geo_revenue/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE ecommerce_gold.geo_revenue;


-- 5. Order Status Funnel
DROP TABLE IF EXISTS ecommerce_gold.order_status_funnel;
CREATE EXTERNAL TABLE ecommerce_gold.order_status_funnel (
    status            STRING,
    order_count       BIGINT,
    total_value       DOUBLE,
    updated_at        TIMESTAMP,
    date_partition    STRING
)
PARTITIONED BY (order_date DATE)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-gold/gold/order_status_funnel/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE ecommerce_gold.order_status_funnel;


-- ============================================================
-- SILVER TABLES (for debugging / data quality checks)
-- ============================================================

DROP TABLE IF EXISTS ecommerce_silver.orders;
CREATE EXTERNAL TABLE ecommerce_silver.orders (
    order_id     STRING,
    customer_id  STRING,
    status       STRING,
    subtotal     DOUBLE,
    shipping     DOUBLE,
    total        DOUBLE,
    currency     STRING,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP,
    channel      STRING,
    promo_code   STRING,
    order_date   DATE,
    order_hour   INT,
    is_promoted  BOOLEAN,
    processed_at TIMESTAMP,
    job_date     STRING
)
PARTITIONED BY (date_partition STRING)
STORED AS PARQUET
LOCATION 's3://ecommerce-pipeline-silver/silver/orders/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

MSCK REPAIR TABLE ecommerce_silver.orders;
