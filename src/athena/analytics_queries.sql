-- ============================================================
-- E-Commerce Analytics Queries
-- Database: ecommerce_gold
-- All queries use partition pruning to minimise Athena cost
-- ============================================================


-- ──────────────────────────────────────────────
-- 1. REVENUE OVERVIEW (last 30 days)
-- ──────────────────────────────────────────────

SELECT
    order_date,
    SUM(gross_revenue)    AS total_revenue,
    SUM(total_orders)     AS total_orders,
    AVG(aov)              AS avg_order_value,
    SUM(total_items_sold) AS items_sold,
    ROUND(AVG(promo_pct), 1) AS avg_promo_pct
FROM ecommerce_gold.daily_revenue_summary
WHERE order_date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY order_date
ORDER BY order_date DESC;


-- ──────────────────────────────────────────────
-- 2. REVENUE BY CHANNEL (last 7 days)
-- ──────────────────────────────────────────────

SELECT
    channel,
    SUM(total_orders)  AS orders,
    SUM(gross_revenue) AS revenue,
    ROUND(AVG(aov), 2) AS aov
FROM ecommerce_gold.daily_revenue_summary
WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY channel
ORDER BY revenue DESC;


-- ──────────────────────────────────────────────
-- 3. WEEK-OVER-WEEK REVENUE GROWTH
-- ──────────────────────────────────────────────

WITH weekly AS (
    SELECT
        DATE_TRUNC('week', order_date) AS week_start,
        SUM(gross_revenue)             AS revenue
    FROM ecommerce_gold.daily_revenue_summary
    WHERE order_date >= DATE_ADD('day', -90, CURRENT_DATE)
    GROUP BY DATE_TRUNC('week', order_date)
)
SELECT
    week_start,
    revenue,
    LAG(revenue) OVER (ORDER BY week_start) AS prev_week_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY week_start))
        / NULLIF(LAG(revenue) OVER (ORDER BY week_start), 0) * 100,
        1
    ) AS wow_growth_pct
FROM weekly
ORDER BY week_start DESC;


-- ──────────────────────────────────────────────
-- 4. TOP 10 PRODUCTS BY REVENUE
-- ──────────────────────────────────────────────

SELECT
    product_id,
    name,
    category,
    units_sold,
    ROUND(revenue, 2)    AS revenue,
    order_count,
    ROUND(margin_pct, 1) AS margin_pct,
    ROUND(rating, 1)     AS rating,
    rank_by_revenue
FROM ecommerce_gold.product_performance
WHERE rank_by_revenue <= 10
ORDER BY rank_by_revenue;


-- ──────────────────────────────────────────────
-- 5. LOW STOCK ALERT
-- ──────────────────────────────────────────────

SELECT
    product_id,
    name,
    category,
    units_sold,
    ROUND(revenue, 2) AS revenue,
    rank_by_revenue
FROM ecommerce_gold.product_performance
WHERE is_low_stock = TRUE
ORDER BY revenue DESC
LIMIT 20;


-- ──────────────────────────────────────────────
-- 6. CUSTOMER SEGMENT DISTRIBUTION
-- ──────────────────────────────────────────────

SELECT
    rfm_segment,
    COUNT(customer_id)          AS customer_count,
    ROUND(AVG(monetary), 2)     AS avg_lifetime_value,
    ROUND(AVG(recency_days), 0) AS avg_recency_days,
    ROUND(AVG(frequency), 1)    AS avg_orders,
    ROUND(SUM(monetary), 2)     AS total_revenue_contribution
FROM ecommerce_gold.customer_rfm
GROUP BY rfm_segment
ORDER BY total_revenue_contribution DESC;


-- ──────────────────────────────────────────────
-- 7. TOP COUNTRIES BY REVENUE (last 30 days)
-- ──────────────────────────────────────────────

SELECT
    country,
    SUM(revenue)             AS total_revenue,
    SUM(orders)              AS total_orders,
    SUM(unique_customers)    AS unique_customers,
    ROUND(AVG(aov), 2)       AS avg_aov
FROM ecommerce_gold.geo_revenue
WHERE order_date >= DATE_ADD('day', -30, CURRENT_DATE)
GROUP BY country
ORDER BY total_revenue DESC
LIMIT 15;


-- ──────────────────────────────────────────────
-- 8. ORDER FUNNEL ANALYSIS (last 7 days)
-- ──────────────────────────────────────────────

WITH totals AS (
    SELECT SUM(order_count) AS grand_total
    FROM ecommerce_gold.order_status_funnel
    WHERE order_date >= DATE_ADD('day', -7, CURRENT_DATE)
)
SELECT
    f.status,
    SUM(f.order_count)                                         AS orders,
    ROUND(SUM(f.total_value), 2)                               AS value,
    ROUND(SUM(f.order_count) * 100.0 / t.grand_total, 1)      AS pct_of_total
FROM ecommerce_gold.order_status_funnel f
CROSS JOIN totals t
WHERE f.order_date >= DATE_ADD('day', -7, CURRENT_DATE)
GROUP BY f.status, t.grand_total
ORDER BY orders DESC;


-- ──────────────────────────────────────────────
-- 9. CHAMPIONS CUSTOMERS (RFM top segment)
-- ──────────────────────────────────────────────

SELECT
    customer_id,
    rfm_segment,
    recency_days,
    frequency,
    ROUND(monetary, 2) AS lifetime_value,
    rfm_score,
    country,
    last_order_date
FROM ecommerce_gold.customer_rfm
WHERE rfm_segment = 'Champions'
ORDER BY monetary DESC
LIMIT 50;


-- ──────────────────────────────────────────────
-- 10. DATA QUALITY CHECK — Silver row counts
-- ──────────────────────────────────────────────

SELECT
    date_partition,
    COUNT(*) AS row_count,
    COUNT(DISTINCT order_id) AS unique_orders,
    SUM(CASE WHEN total IS NULL THEN 1 ELSE 0 END) AS null_totals,
    MIN(total) AS min_total,
    MAX(total) AS max_total
FROM ecommerce_silver.orders
WHERE date_partition >= DATE_FORMAT(DATE_ADD('day', -7, CURRENT_DATE), '%Y-%m-%d')
GROUP BY date_partition
ORDER BY date_partition DESC;
