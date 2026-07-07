# 🛒 AWS E-Commerce Analytics Pipeline

A production-grade **Medallion Architecture** data pipeline built entirely on AWS Free Tier services.  
Ingests raw e-commerce events → cleans and enriches → aggregates into business metrics → queryable via SQL.

---

## 🏗️ Architecture

```
Data Sources
  ├── Python generator (streaming orders, customers, products)
  └── CSV batch uploads
        │
        ▼
  Ingestion Layer
  ├── Amazon Kinesis Data Firehose  (streaming events → S3)
  └── AWS Lambda                   (batch CSV trigger)
        │
        ▼
  S3 Data Lake (Medallion)
  ├── 🥉 Bronze  s3://bucket/bronze/   Raw JSON/CSV, partitioned by date
  ├── 🥈 Silver  s3://bucket/silver/   Cleaned Parquet, typed, validated
  └── 🥇 Gold    s3://bucket/gold/     Aggregated Parquet, business KPIs
        │
        ▼
  Processing (AWS Glue)
  ├── Glue Job: bronze_to_silver.py   Clean, deduplicate, type-cast
  ├── Glue Job: silver_to_gold.py     Aggregate KPIs, joins
  └── Glue Data Catalog               Schema registry for all layers
        │
        ▼
  Analytics
  └── Amazon Athena                   SQL queries on Gold (pay-per-query)
        │
        ▼
  Orchestration & Monitoring
  ├── AWS Step Functions              Full pipeline DAG with retries
  ├── Amazon CloudWatch               Metrics, alarms, dashboards
  ├── AWS CloudFormation              Infrastructure as Code
  └── GitHub Actions                  CI/CD (lint → test → deploy)
```

---

## 📊 Dataset: Synthetic E-Commerce

| Table         | Fields                                                                 | Volume    |
|---------------|------------------------------------------------------------------------|-----------|
| `orders`      | order_id, customer_id, product_id, quantity, price, status, timestamp  | ~10K rows |
| `customers`   | customer_id, name, email, country, signup_date, segment                | ~1K rows  |
| `products`    | product_id, name, category, price, stock_qty, supplier                 | ~500 rows |
| `order_items` | item_id, order_id, product_id, quantity, unit_price, discount          | ~30K rows |

---

## 🛠️ AWS Services Used (Free Tier)

| Service                    | Usage in Project                          | Free Tier Limit              |
|----------------------------|-------------------------------------------|------------------------------|
| **Amazon S3**              | Data lake (Bronze/Silver/Gold)            | 5 GB storage, 2K PUT/month   |
| **AWS Glue**               | ETL jobs + Data Catalog                   | 1M DPU-hours (first year)    |
| **Amazon Athena**          | SQL analytics on Gold layer               | 1 TB queries/month           |
| **AWS Lambda**             | Batch trigger + pipeline events           | 1M requests/month            |
| **Amazon Kinesis Firehose**| Streaming ingest to S3                    | 500 MB/month free             |
| **AWS Step Functions**     | Pipeline orchestration                    | 4K state transitions/month   |
| **Amazon CloudWatch**      | Metrics, alarms, dashboards               | 10 metrics, 10 alarms        |
| **AWS CloudFormation**     | Infrastructure as Code                    | Free (pay for resources)     |
| **AWS IAM**                | Least-privilege roles for each service    | Free                         |

---

## 🚀 Quick Start

### Prerequisites
- AWS CLI configured (`aws configure`)
- Python 3.11+
- Git

### 1. Clone & install dependencies
```bash
git clone https://github.com/YOUR_USERNAME/aws-ecommerce-pipeline.git
cd aws-ecommerce-pipeline
pip install -r src/data_generator/requirements.txt
```

### 2. Deploy infrastructure
```bash
chmod +x scripts/*.sh
./scripts/setup.sh      # Creates S3 bucket, IAM roles
./scripts/deploy.sh     # Deploys full CloudFormation stack
```

### 3. Generate & ingest data
```bash
python src/data_generator/generate_data.py --mode stream --events 1000
python src/data_generator/generate_data.py --mode batch  --rows 5000
```

### 4. Run the pipeline
```bash
./scripts/run_pipeline.sh
# Or trigger via Step Functions console
```

### 5. Query results in Athena
```sql
-- Run in Athena console (database: ecommerce_gold)
SELECT * FROM daily_revenue_summary
ORDER BY order_date DESC
LIMIT 30;
```

---

## 📁 Project Structure

```
aws-ecommerce-pipeline/
├── .github/workflows/deploy.yml       # CI/CD pipeline
├── infrastructure/
│   ├── cloudformation/
│   │   ├── main-stack.yaml            # Root stack (nested)
│   │   ├── s3-stack.yaml              # S3 buckets + lifecycle policies
│   │   ├── glue-stack.yaml            # Glue jobs, crawlers, catalog
│   │   └── lambda-stack.yaml          # Lambda functions + triggers
│   └── iam/roles.yaml                 # IAM roles & policies
├── src/
│   ├── data_generator/
│   │   ├── generate_data.py           # Synthetic data producer
│   │   └── requirements.txt
│   ├── lambda/
│   │   ├── batch_ingestion/handler.py # S3-triggered batch loader
│   │   └── pipeline_trigger/handler.py# Step Functions starter
│   ├── glue/
│   │   ├── bronze_to_silver.py        # Cleaning ETL job
│   │   └── silver_to_gold.py          # Aggregation ETL job
│   ├── step_functions/
│   │   └── pipeline_definition.json   # State machine definition
│   └── athena/
│       ├── create_tables.sql          # External table DDLs
│       └── analytics_queries.sql      # Business intelligence queries
├── scripts/
│   ├── setup.sh                       # One-time setup
│   ├── deploy.sh                      # Deploy/update stack
│   └── run_pipeline.sh                # Trigger pipeline run
├── tests/
│   ├── test_data_generator.py
│   └── test_glue_jobs.py
├── notebooks/
│   └── analytics_exploration.ipynb    # EDA on Gold layer
└── docs/architecture.md
```

---

## 📈 Actual Pipeline Results

| Metric | Value |
|--------|-------|
| Total Orders Processed | 4,197 |
| Total Revenue Generated | $7,568,959.45 |
| Average Order Value | $1,803.42 |
| Countries Tracked | 10 |
| Channels | web, mobile, api |
| Top Country | UK ($916,402) |
| Top Channel | API ($2,604,244) |
| Pipeline Status | ✅ COMPLETE |

### Revenue by Country
| Country | Orders | Revenue |
|---------|--------|---------|
| UK | 485 | $916,402 |
| MX | 489 | $905,767 |
| AU | 485 | $848,475 |
| DE | 453 | $836,179 |
| JP | 461 | $813,909 |

### Order Status Funnel
| Status | Count | % |
|--------|-------|---|
| Returned | 877 | 17.5% |
| Pending | 853 | 17.1% |
| Delivered | 835 | 16.7% |
| Shipped | 825 | 16.5% |
| Processing | 807 | 16.1% |
| Cancelled | 803 | 16.1% |

---

## 💰 Estimated Cost

With 179 credits and 13 days:

| Service          | Estimated Cost |
|------------------|----------------|
| S3 (5 GB)        | ~$0.12         |
| Glue (10 DPU-hr) | ~$0.44         |
| Athena (1 GB)    | ~$0.005        |
| Lambda           | $0.00 (free)   |
| Kinesis Firehose | $0.00 (free)   |
| Step Functions   | $0.00 (free)   |
| **Total**        | **~$1–5**      |

---

## 🧪 Running Tests

```bash
pip install pytest moto boto3
pytest tests/ -v
```

---

## 🗑️ Teardown

```bash
./scripts/setup.sh --destroy
# Deletes all AWS resources to avoid charges
```

---

## 📄 License

MIT — free to use for portfolio, learning, or production.

👨‍💻 Author
Pratham Patil — Data Engineer

GitHub: @Prathampatil08
