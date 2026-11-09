-- AWS E-commerce Data Pipeline
-- Post-ingestion data quality checks
-- Source: Apache Iceberg table in Athena

-- 1. Dataset completeness and revenue
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(DISTINCT CAST(order_date AS DATE)) AS reporting_days,
    ROUND(SUM(amount), 2) AS total_revenue
FROM "project1-ecommerce-etl-data-catalog".orders_iceberg;


-- 2. Duplicate primary keys
-- Expected: zero rows
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM "project1-ecommerce-etl-data-catalog".orders_iceberg
GROUP BY order_id
HAVING COUNT(*) > 1;


-- 3. Missing or invalid required fields
-- Expected: zero invalid records
SELECT
    COUNT(*) AS invalid_records
FROM "project1-ecommerce-etl-data-catalog".orders_iceberg
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR order_date IS NULL
   OR TRY_CAST(order_date AS DATE) IS NULL
   OR amount IS NULL
   OR amount <= 0;


-- 4. Daily reporting reconciliation
-- Each reporting day should appear once in the view
SELECT
    COUNT(*) AS reporting_days,
    COUNT(DISTINCT order_day) AS unique_reporting_days,
    ROUND(SUM(daily_revenue), 2) AS total_daily_revenue,
    MAX(cumulative_revenue) AS final_cumulative_revenue
FROM "project1-ecommerce-etl-data-catalog".daily_revenue_report;
