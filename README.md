# Event-Driven AWS E-commerce Data Pipeline

An end-to-end AWS data engineering project featuring event-driven ingestion, incremental processing, Apache Iceberg UPSERT operations, SQL analytics, and business intelligence.

## Project Overview

This project implements an e-commerce analytics pipeline using AWS managed services. It processes synthetic order data delivered as CSV files, maintains an Apache Iceberg table, and exposes revenue analytics through Amazon Athena and QuickSight.

## Architecture

See [Architecture and Data Flow](docs/architecture.md) for the system diagram and explanation.

## Technology Stack

| Technology | Purpose |
|---|---|
| Amazon S3 | Raw ingestion and Iceberg storage |
| Amazon EventBridge | Event routing and failure detection |
| AWS Glue | PySpark processing and workflow orchestration |
| Apache Iceberg | Transactional data lake table |
| AWS Glue Data Catalog | Table metadata |
| Amazon Athena | Serverless SQL analytics |
| Amazon QuickSight | Revenue dashboards |
| Amazon SNS | Failure notifications |
| Python | Synthetic data generation |

## Key Engineering Features

- **Incremental ETL:** AWS Glue job bookmarks identify new input files.
- **Iceberg UPSERT:** Spark SQL MERGE updates existing orders and inserts new ones.
- **Event-Driven Processing:** EventBridge triggers a Glue workflow with event batching.
- **Data Quality:** SQL checks validate uniqueness, required fields, and reporting totals.
- **Analytics:** Athena calculates daily and cumulative revenue.
- **Monitoring:** EventBridge and SNS provide Glue failure notifications.

## Validated Results

| Metric | Result |
|---|---:|
| Synthetic orders | 800 |
| Unique order IDs | 800 |
| Reporting dates | 40 |
| Synthetic revenue | $88,754.93 |
| Glue execution time | 73 seconds |
| Glue compute usage | 147 DPU-seconds |

See [Performance Analysis](docs/performance.md).

## Repository Structure

- `generate_orders.py` — Synthetic data generator
- `sample_data/` — Synthetic CSV batches
- `glue_jobs/` — PySpark ETL script and configuration
- `sql/` — Athena queries and schema metadata
- `infrastructure/` — AWS resource configuration exports
- `docs/` — Architecture and performance documentation

## Limitations

This is an educational portfolio implementation, not a production deployment. The performance measurements are based on a small synthetic dataset, and the exported AWS resource definitions are not complete infrastructure-as-code templates.

## Future Improvements

- Deployable AWS infrastructure using CDK
- Automated CI/CD and integration tests
- Larger workload benchmarks
- Schema evolution testing
- Improved workflow concurrency and failure recovery

## Data Disclaimer

All orders, customers, and revenue figures are synthetically generated for educational purposes.