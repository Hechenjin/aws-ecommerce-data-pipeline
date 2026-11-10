# AWS Glue and Apache Iceberg Integration Tests

## Overview

This project validates the incremental ETL implementation through both local automated testing and isolated AWS integration testing.

The integration environment uses a separate AWS Glue job, dedicated S3 input prefix, and Apache Iceberg test table to avoid modifying the existing analytical dataset.

## Test Environment

| Component | Configuration |
|---|---|
| Runtime | AWS Glue 6.0 |
| Processing engine | PySpark |
| Python validation module | `src/order_transformations.py` |
| Glue test job | `Ecommerce-Iceberg-Upsert-Test` |
| Test table | `orders_iceberg_integration_test` |
| Storage format | Apache Iceberg |
| Query engine | Amazon Athena |
| Log monitoring | Amazon CloudWatch |

## Automated Unit Testing

The local test suite contains 16 passing tests:

- 7 dataset validation tests
- 9 PySpark transformation tests

The tests cover schema validation, record uniqueness, positive order amounts, missing values, invalid dates, and duplicate order IDs.

GitHub Actions is configured to run automated tests.

## AWS Integration Tests

| Test | Scenario | Verified outcome |
|---|---|---|
| INSERT | Process a new order | New record inserted |
| UPDATE | Update the existing order's amount | Amount changed without creating a duplicate |
| Bookmark | Run without uploading new input | No new records processed; MERGE skipped |
| Replay | Process the same logical order through a new CSV object | One record remains with the expected amount |
| Invalid input | Submit an order with a negative amount | Validation raises `ValueError` |
| Data integrity | Query