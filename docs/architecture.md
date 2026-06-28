# Architecture Documentation

## Overview

This pipeline implements the **Medallion Architecture** (Bronze → Silver → Gold),
a widely adopted pattern in modern data lakehouses.

---

## Data Flow

```
[Python Generator] ──► [Kinesis Firehose] ──► [S3 Bronze]
[CSV Upload]       ──► [Lambda Trigger]   ──► [S3 Bronze]
                                                    │
                                              [Glue ETL #1]
                                           bronze_to_silver.py
                                                    │
                                              [S3 Silver]
                                           (clean Parquet)
                                                    │
                                              [Glue ETL #2]
                                           silver_to_gold.py
                                                    │
                                              [S3 Gold]
                                          (aggregated Parquet)
                                                    │
                                         [Glue Crawler] ─► [Glue Catalog]
                                                    │
                                              [Athena SQL]
                                         Analytics queries
```

---

## S3 Bucket Layout

```
ecommerce-pipeline-bronze-{account}/
  bronze/
    orders/
      dt=2024/11/01/orders.csv
    customers/
      dt=2024/11/01/customers.csv
    products/
      dt=2024/11/01/products.csv
    order_items/
      dt=2024/11/01/order_items.csv

ecommerce-pipeline-silver-{account}/
  silver/
    orders/
      date_partition=2024-11-01/
        part-00000.parquet
    customers/  ...
    products/   ...
    order_items/ ...

ecommerce-pipeline-gold-{account}/
  gold/
    daily_revenue_summary/
      order_date=2024-11-01/
        part-00000.parquet
    customer_rfm/
    product_performance/
      category=Electronics/
    geo_revenue/
    order_status_funnel/
```

---

## Glue Job Details

### bronze_to_silver.py

| Step | Operation |
|------|-----------|
| 1 | Read CSV from Bronze with explicit schemas |
| 2 | Type cast all columns (doubles, timestamps, ints) |
| 3 | Normalise strings (lower, trim, initcap) |
| 4 | Validate: drop null PKs, negative totals, invalid emails |
| 5 | Deduplicate on primary key (latest `updated_at` wins) |
| 6 | Add derived columns (order_date, order_hour, is_promoted, margin_pct) |
| 7 | Add audit columns (processed_at, job_date, date_partition) |
| 8 | Write Parquet (SNAPPY compressed) partitioned by date |

### silver_to_gold.py

| Gold Table | Logic |
|------------|-------|
| `daily_revenue_summary` | Group by order_date + channel; sum revenue, count orders, compute AOV and promo% |
| `customer_rfm` | Compute Recency/Frequency/Monetary scores; assign Champion/Loyal/At Risk segments |
| `product_performance` | Aggregate units sold, revenue per product; rank by revenue and units |
| `geo_revenue` | Group by country + date; sum revenue, unique customers |
| `order_status_funnel` | Count orders per status per day |

---

## Step Functions State Machine

```
ValidateInput
     │
RunBronzeToSilver  ──(fail)──► HandleFailure ──► PipelineFailed
     │
RunSilverToGold    ──(fail)──► HandleFailure
     │
RunGlueCrawler
     │
WaitForCrawler (60s)
     │
CheckCrawlerStatus
     │
IsCrawlerDone ──(not done)──► WaitForCrawler
     │
PublishSuccessMetric ──► PipelineSucceeded
```

Retries: each Glue stage retries up to 3 times with exponential backoff.

---

## CloudWatch Monitoring

| Metric | Namespace | Alarm |
|--------|-----------|-------|
| `PipelineSuccess` | EcommercePipeline | — |
| `PipelineFailure` | EcommercePipeline | Triggers SNS email alert |
| `FilesIngested`   | EcommercePipeline | — |
| `BytesIngested`   | EcommercePipeline | — |
| `IngestionErrors` | EcommercePipeline | — |

---

## Cost Optimisation

- **Parquet + SNAPPY** compression reduces Athena scan costs by ~75% vs CSV.
- **Partition pruning** on `order_date` means queries only read relevant partitions.
- **S3 lifecycle rules** move Bronze data to STANDARD_IA after 30 days (67% cheaper).
- **Athena query size limit** (1 GB) prevents accidental full-table scans.
- **Glue G.1X workers** (2 DPU) are the minimum — sufficient for datasets < 10 GB.
- **Lambda** is effectively free at this scale (well within 1M free requests/month).
