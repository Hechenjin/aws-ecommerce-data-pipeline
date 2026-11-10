
CREATE TABLE "project1-ecommerce-etl-data-catalog".orders_iceberg_integration_test
WITH (
    table_type = 'ICEBERG',
    is_external = false,
    format = 'PARQUET',
    location = 's3://etl-pineline-project1-753397111997-us-east-1-an/iceberg/integration-test-orders/'
)
AS
SELECT
    order_id,
    customer_id,
    amount,
    order_date,
    country
FROM "project1-ecommerce-etl-data-catalog".orders_iceberg
WITH NO DATA
