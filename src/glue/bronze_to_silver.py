"""
AWS Glue ETL Job: Bronze → Silver
Reads raw CSV/JSON from the Bronze S3 layer.
Applies cleaning, type-casting, deduplication, and validation.
Writes clean Parquet to the Silver S3 layer, partitioned by date.

Run this job from Glue console or via Step Functions.
"""

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType
)

# ──────────────────────────────────────────────
# INIT
# ──────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "BRONZE_BUCKET",
    "SILVER_BUCKET",
    "PROCESSING_DATE",   # YYYY-MM-DD  (injected by Step Functions)
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_BUCKET   = args["BRONZE_BUCKET"]
SILVER_BUCKET   = args["SILVER_BUCKET"]
PROC_DATE       = args.get("PROCESSING_DATE", datetime.utcnow().strftime("%Y-%m-%d"))
DT_PARTITION    = PROC_DATE.replace("-", "/")

print(f"[bronze→silver] processing date: {PROC_DATE}")


# ──────────────────────────────────────────────
# SCHEMAS  (enforce types early — fail fast)
# ──────────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("order_id",    StringType(),    False),
    StructField("customer_id", StringType(),    False),
    StructField("status",      StringType(),    True),
    StructField("subtotal",    DoubleType(),    True),
    StructField("shipping",    DoubleType(),    True),
    StructField("total",       DoubleType(),    True),
    StructField("currency",    StringType(),    True),
    StructField("created_at",  TimestampType(), True),
    StructField("updated_at",  TimestampType(), True),
    StructField("channel",     StringType(),    True),
    StructField("promo_code",  StringType(),    True),
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id",  StringType(), False),
    StructField("name",         StringType(), True),
    StructField("email",        StringType(), True),
    StructField("country",      StringType(), True),
    StructField("signup_date",  StringType(), True),
    StructField("segment",      StringType(), True),
    StructField("phone",        StringType(), True),
    StructField("city",         StringType(), True),
])

PRODUCT_SCHEMA = StructType([
    StructField("product_id",    StringType(), False),
    StructField("name",          StringType(), True),
    StructField("category",      StringType(), True),
    StructField("price",         DoubleType(), True),
    StructField("cost",          DoubleType(), True),
    StructField("stock_qty",     IntegerType(), True),
    StructField("supplier",      StringType(), True),
    StructField("sku",           StringType(), True),
    StructField("rating",        DoubleType(), True),
    StructField("review_count",  IntegerType(), True),
])

ORDER_ITEM_SCHEMA = StructType([
    StructField("item_id",    StringType(), False),
    StructField("order_id",   StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("quantity",   IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount",   DoubleType(), True),
    StructField("line_total", DoubleType(), True),
    StructField("created_at", TimestampType(), True),
])


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def read_csv(path: str, schema=None):
    reader = spark.read.option("header", "true").option("inferSchema", "false")
    if schema:
        reader = reader.schema(schema)
    return reader.csv(path)


def write_parquet(df, table: str, partition_col: str = "date_partition"):
    out_path = f"s3://{SILVER_BUCKET}/silver/{table}/"
    (df
     .write
     .mode("overwrite")
     .partitionBy(partition_col)
     .parquet(out_path))
    count = df.count()
    print(f"  ✔ Wrote {count:,} rows → {out_path}  (partition: {partition_col})")
    return count


def add_audit_cols(df):
    return (df
            .withColumn("processed_at", F.current_timestamp())
            .withColumn("job_date",     F.lit(PROC_DATE))
            .withColumn("date_partition", F.lit(PROC_DATE)))


# ──────────────────────────────────────────────
# ORDERS
# ──────────────────────────────────────────────

def process_orders():
    print("\n[orders] reading bronze …")
    raw = read_csv(f"s3://{BRONZE_BUCKET}/bronze/orders/dt={DT_PARTITION}/")

    df = (raw
          # Normalise column names
          .withColumnRenamed("order_id",    "order_id")
          # Type coercions
          .withColumn("subtotal",  F.col("subtotal").cast(DoubleType()))
          .withColumn("shipping",  F.col("shipping").cast(DoubleType()))
          .withColumn("total",     F.col("total").cast(DoubleType()))
          .withColumn("created_at", F.to_timestamp("created_at"))
          .withColumn("updated_at", F.to_timestamp("updated_at"))
          # Cleaning
          .withColumn("status",    F.lower(F.trim(F.col("status"))))
          .withColumn("currency",  F.upper(F.trim(F.col("currency"))))
          .withColumn("channel",   F.lower(F.trim(F.col("channel"))))
          # Derived
          .withColumn("order_date", F.to_date("created_at"))
          .withColumn("order_hour", F.hour("created_at"))
          .withColumn("is_promoted", F.col("promo_code").isNotNull())
          # Validation: drop rows with null PKs or impossible totals
          .filter(F.col("order_id").isNotNull())
          .filter(F.col("customer_id").isNotNull())
          .filter(F.col("total") >= 0)
          # Deduplication (keep latest updated_at per order_id)
          )

    dedup = (df
             .withColumn("_rn", F.row_number().over(
                 __import__("pyspark.sql.window", fromlist=["Window"])
                 .Window.partitionBy("order_id").orderBy(F.desc("updated_at"))))
             .filter(F.col("_rn") == 1)
             .drop("_rn"))

    final = add_audit_cols(dedup)
    return write_parquet(final, "orders")


# ──────────────────────────────────────────────
# CUSTOMERS
# ──────────────────────────────────────────────

def process_customers():
    print("\n[customers] reading bronze …")
    raw = read_csv(f"s3://{BRONZE_BUCKET}/bronze/customers/dt={DT_PARTITION}/")

    df = (raw
          .withColumn("email",       F.lower(F.trim(F.col("email"))))
          .withColumn("country",     F.upper(F.trim(F.col("country"))))
          .withColumn("segment",     F.initcap(F.trim(F.col("segment"))))
          .withColumn("signup_date", F.to_date("signup_date"))
          # Remove rows with missing PK or invalid email
          .filter(F.col("customer_id").isNotNull())
          .filter(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"))
          # Dedup on customer_id
          .dropDuplicates(["customer_id"]))

    final = add_audit_cols(df)
    return write_parquet(final, "customers")


# ──────────────────────────────────────────────
# PRODUCTS
# ──────────────────────────────────────────────

def process_products():
    print("\n[products] reading bronze …")
    raw = read_csv(f"s3://{BRONZE_BUCKET}/bronze/products/dt={DT_PARTITION}/")

    df = (raw
          .withColumn("price",        F.col("price").cast(DoubleType()))
          .withColumn("cost",         F.col("cost").cast(DoubleType()))
          .withColumn("stock_qty",    F.col("stock_qty").cast(IntegerType()))
          .withColumn("rating",       F.col("rating").cast(DoubleType()))
          .withColumn("review_count", F.col("review_count").cast(IntegerType()))
          .withColumn("category",     F.initcap(F.trim(F.col("category"))))
          .withColumn("sku",          F.upper(F.trim(F.col("sku"))))
          # Margin calculation
          .withColumn("margin_pct",
                      F.when(F.col("price") > 0,
                             F.round((F.col("price") - F.col("cost")) / F.col("price") * 100, 2))
                      .otherwise(F.lit(None)))
          .withColumn("is_low_stock", F.col("stock_qty") < 10)
          .filter(F.col("product_id").isNotNull())
          .filter(F.col("price") > 0)
          .dropDuplicates(["product_id"]))

    final = add_audit_cols(df)
    return write_parquet(final, "products")


# ──────────────────────────────────────────────
# ORDER ITEMS
# ──────────────────────────────────────────────

def process_order_items():
    print("\n[order_items] reading bronze …")
    raw = read_csv(f"s3://{BRONZE_BUCKET}/bronze/order_items/dt={DT_PARTITION}/")

    df = (raw
          .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
          .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
          .withColumn("discount",   F.col("discount").cast(DoubleType()))
          .withColumn("line_total", F.col("line_total").cast(DoubleType()))
          .withColumn("created_at", F.to_timestamp("created_at"))
          # Recalculate line_total as source of truth
          .withColumn("line_total_recalc",
                      F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")), 2))
          # Flag discrepancies > $0.01
          .withColumn("total_mismatch",
                      F.abs(F.col("line_total") - F.col("line_total_recalc")) > 0.01)
          .filter(F.col("item_id").isNotNull())
          .filter(F.col("order_id").isNotNull())
          .filter(F.col("quantity") > 0)
          .dropDuplicates(["item_id"]))

    final = add_audit_cols(df)
    return write_parquet(final, "order_items")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    results = {}
    results["orders"]      = process_orders()
    results["customers"]   = process_customers()
    results["products"]    = process_products()
    results["order_items"] = process_order_items()

    print("\n" + "="*50)
    print("Bronze → Silver complete")
    for table, count in results.items():
        print(f"  {table:15s} {count:>10,} rows")
    print("="*50)


main()
job.commit()
