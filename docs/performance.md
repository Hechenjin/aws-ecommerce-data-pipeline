# Performance Analysis

## Dataset Validation

| Metric | Result |
|---|---:|
| Total synthetic orders | 800 |
| Unique order IDs | 800 |
| Reporting dates | 40 |
| Synthetic revenue | $88,754.93 |

## AWS Glue Benchmark

| Metric | Result |
|---|---:|
| Runtime | AWS Glue 6.0 |
| Worker type | G.1X |
| Worker count | 2 |
| Execution time | 73 seconds |
| DPU consumption | 147 DPU-seconds |
| Equivalent compute | 0.04083 DPU-hours |

## Apache Iceberg Storage

| Metric | Result |
|---|---:|
| Active data files | 6 |
| Total active data-file size | 14.16 KB |
| Average data-file size | 2.36 KB |
| Observed historical snapshots | 8 |

## Athena Performance

A validation query scanned approximately 5.05 KB of data and completed in approximately 0.8 seconds.

## Engineering Findings

- Fixed AWS Glue job initialization overhead appears significant at this dataset size.
- Iceberg compaction was not performed because the dataset was too small to establish a meaningful benefit.
- Iceberg snapshots provide historical table versions for time-travel queries.
- Larger datasets and repeated tests are necessary to evaluate production-scale throughput and costs.

These measurements describe a controlled synthetic workload rather than production performance.