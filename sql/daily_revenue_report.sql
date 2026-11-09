CREATE VIEW "project1-ecommerce-etl-data-catalog".daily_revenue_report AS
WITH
  daily_sales AS (
   SELECT
     CAST(order_date AS DATE) order_day
   , SUM(amount) daily_revenue
   FROM
     orders_iceberg
   GROUP BY CAST(order_date AS DATE)
) 
SELECT
  order_day
, daily_revenue
, ROUND(SUM(daily_revenue) OVER (ORDER BY order_day ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) cumulative_revenue
FROM
  daily_sales
ORDER BY order_day ASC
