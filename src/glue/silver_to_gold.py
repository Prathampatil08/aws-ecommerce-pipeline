"""
AWS Glue ETL Job: Silver → Gold
Reads clean Parquet from the Silver S3 layer.
Computes business KPIs and aggregations.
Writes Gold Parquet tables, optimised for Athena queries.

Gold tables produced:
  - daily_revenue_summary     (date × channel)
  - customer_rfm              (RFM segments)
  - product_performance       (top sellers, margin ranking)
  - geo_revenue               (country breakdown)
  - order_status_funnel       (funnel by date)
"""

import sys
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F, Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SILVER_BUCKET",
    "GOLD_BUCKET",
    "PROCESSING_DATE",
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

SILVER_BUCKET = args["SILVER_BUCKET"]
GOLD_BUCKET   = args["GOLD_BUCKET"]
PROC_DATE     = args.get("PROCESSING_DATE", datetime.utcnow().strftime("%Y-%m-%d"))

print(f"[silver→gold] processing date: {PROC_DATE}")


# ──────────────────────────────────────────────
# READERS
# ──────────────────────────────────────────────

def read_silver(table: str):
    path = f"s3://{SILVER_BUCKET}/silver/{table}/"
    df = spark.read.parquet(path)
    print(f"  ✔ Loaded {table}: {df.count():,} rows")
    return df


def write_gold(df, table: str, partition_col: str = "date_partition"):
    out = f"s3://{GOLD_BUCKET}/gold/{table}/"
    (df
     .write
     .mode("overwrite")
     .partitionBy(partition_col)
     .parquet(out))
    count = df.count()
    print(f"  ✔ Gold/{table}: {count:,} rows → {out}")
    return count


def add_meta(df):
    return (df
            .withColumn("updated_at",    F.current_timestamp())
            .withColumn("date_partition", F.lit(PROC_DATE)))


# ──────────────────────────────────────────────
# 1. DAILY REVENUE SUMMARY
# ──────────────────────────────────────────────

def build_daily_revenue(orders, order_items):
    print("\n[gold] building daily_revenue_summary …")

    # Join items → orders to get channel & status per item
    enriched = (order_items
                .join(orders.select("order_id", "status", "channel",
                                    "order_date", "is_promoted"),
                      on="order_id", how="inner"))

    daily = (enriched
             .filter(F.col("status") != "cancelled")
             .groupBy("order_date", "channel")
             .agg(
                 F.countDistinct("order_id").alias("total_orders"),
                 F.sum("line_total").alias("gross_revenue"),
                 F.sum(F.when(F.col("is_promoted"), F.col("line_total"))).alias("promo_revenue"),
                 F.avg("line_total").alias("avg_item_value"),
                 F.countDistinct("item_id").alias("total_items_sold"),
                 F.sum("quantity").alias("total_units"),
             )
             .withColumn("gross_revenue",  F.round("gross_revenue", 2))
             .withColumn("promo_revenue",  F.round("promo_revenue", 2))
             .withColumn("avg_item_value", F.round("avg_item_value", 2))
             .withColumn("aov",            F.round(F.col("gross_revenue") / F.col("total_orders"), 2))
             .withColumn("promo_pct",
                         F.round(F.col("promo_revenue") / F.col("gross_revenue") * 100, 1))
             .orderBy("order_date", "channel"))

    return write_gold(add_meta(daily), "daily_revenue_summary", "order_date")


# ──────────────────────────────────────────────
# 2. CUSTOMER RFM
# ──────────────────────────────────────────────

def build_customer_rfm(orders, customers):
    print("\n[gold] building customer_rfm …")

    snapshot = datetime.strptime(PROC_DATE, "%Y-%m-%d")

    rfm_raw = (orders
               .filter(F.col("status") != "cancelled")
               .groupBy("customer_id")
               .agg(
                   F.max("order_date").alias("last_order_date"),
                   F.countDistinct("order_id").alias("frequency"),
                   F.sum("total").alias("monetary"),
               )
               .withColumn("recency_days",
                           F.datediff(F.lit(snapshot), F.col("last_order_date")))
               .withColumn("monetary", F.round("monetary", 2)))

    # Score each dimension 1–4
    for col_name, new_col, ascending in [
        ("recency_days", "r_score", True),   # lower recency = better
        ("frequency",    "f_score", False),
        ("monetary",     "m_score", False),
    ]:
        q = rfm_raw.approxQuantile(col_name, [0.25, 0.5, 0.75], 0.05)
        lo, mid, hi = q[0], q[1], q[2]
        if ascending:  # lower value → higher score
            rfm_raw = (rfm_raw.withColumn(new_col,
                        F.when(F.col(col_name) <= lo, 4)
                         .when(F.col(col_name) <= mid, 3)
                         .when(F.col(col_name) <= hi, 2)
                         .otherwise(1)))
        else:          # higher value → higher score
            rfm_raw = (rfm_raw.withColumn(new_col,
                        F.when(F.col(col_name) >= hi, 4)
                         .when(F.col(col_name) >= mid, 3)
                         .when(F.col(col_name) >= lo, 2)
                         .otherwise(1)))

    rfm = (rfm_raw
           .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
           .withColumn("rfm_segment",
               F.when(F.col("rfm_score") >= 10, "Champions")
                .when(F.col("rfm_score") >= 8,  "Loyal Customers")
                .when(F.col("rfm_score") >= 6,  "Potential Loyalists")
                .when(F.col("rfm_score") >= 4,  "At Risk")
                .otherwise("Lost"))
           .join(customers.select("customer_id", "country", "segment", "signup_date"),
                 on="customer_id", how="left"))

    return write_gold(add_meta(rfm), "customer_rfm")


# ──────────────────────────────────────────────
# 3. PRODUCT PERFORMANCE
# ──────────────────────────────────────────────

def build_product_performance(order_items, orders, products):
    print("\n[gold] building product_performance …")

    active_orders = orders.filter(F.col("status") != "cancelled").select("order_id", "order_date")

    perf = (order_items
            .join(active_orders, on="order_id", how="inner")
            .groupBy("product_id")
            .agg(
                F.sum("quantity").alias("units_sold"),
                F.sum("line_total").alias("revenue"),
                F.countDistinct("order_id").alias("order_count"),
                F.avg("discount").alias("avg_discount"),
                F.max("order_date").alias("last_sold_date"),
            )
            .join(products.select("product_id", "name", "category",
                                  "price", "cost", "margin_pct",
                                  "rating", "review_count", "is_low_stock"),
                  on="product_id", how="left")
            .withColumn("revenue",      F.round("revenue", 2))
            .withColumn("avg_discount", F.round("avg_discount", 4))
            .withColumn("rank_by_revenue",
                        F.rank().over(Window.orderBy(F.desc("revenue"))))
            .withColumn("rank_by_units",
                        F.rank().over(Window.orderBy(F.desc("units_sold"))))
            )

    return write_gold(add_meta(perf), "product_performance", "category")


# ──────────────────────────────────────────────
# 4. GEOGRAPHIC REVENUE
# ──────────────────────────────────────────────

def build_geo_revenue(orders, customers):
    print("\n[gold] building geo_revenue …")

    geo = (orders
           .filter(F.col("status") != "cancelled")
           .join(customers.select("customer_id", "country"), on="customer_id", how="left")
           .groupBy("country", "order_date")
           .agg(
               F.sum("total").alias("revenue"),
               F.countDistinct("order_id").alias("orders"),
               F.countDistinct("customer_id").alias("unique_customers"),
               F.avg("total").alias("aov"),
           )
           .withColumn("revenue", F.round("revenue", 2))
           .withColumn("aov",     F.round("aov", 2))
           .orderBy("country", "order_date"))

    return write_gold(add_meta(geo), "geo_revenue", "order_date")


# ──────────────────────────────────────────────
# 5. ORDER STATUS FUNNEL
# ──────────────────────────────────────────────

def build_order_funnel(orders):
    print("\n[gold] building order_status_funnel …")

    funnel = (orders
              .groupBy("order_date", "status")
              .agg(
                  F.count("order_id").alias("order_count"),
                  F.sum("total").alias("total_value"),
              )
              .withColumn("total_value", F.round("total_value", 2))
              .orderBy("order_date", "status"))

    return write_gold(add_meta(funnel), "order_status_funnel", "order_date")


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    print("\nLoading silver tables …")
    orders      = read_silver("orders")
    customers   = read_silver("customers")
    products    = read_silver("products")
    order_items = read_silver("order_items")

    # Cache frequently joined frames
    orders.cache()
    order_items.cache()

    results = {}
    results["daily_revenue_summary"] = build_daily_revenue(orders, order_items)
    results["customer_rfm"]          = build_customer_rfm(orders, customers)
    results["product_performance"]   = build_product_performance(order_items, orders, products)
    results["geo_revenue"]           = build_geo_revenue(orders, customers)
    results["order_status_funnel"]   = build_order_funnel(orders)

    print("\n" + "="*50)
    print("Silver → Gold complete")
    for table, count in results.items():
        print(f"  {table:30s} {count:>8,} rows")
    print("="*50)


main()
job.commit()
