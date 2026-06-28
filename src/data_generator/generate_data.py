"""
E-Commerce Synthetic Data Generator
Produces realistic orders, customers, products, and order_items.
Supports two modes:
  --mode batch   : writes CSV files to local disk, then uploads to S3 Bronze
  --mode stream  : sends JSON events to Kinesis Firehose in real time
"""

import argparse
import json
import random
import uuid
import boto3
import csv
import os
import time
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)

# ──────────────────────────────────────────────
# CONFIG  (override via env vars or CLI flags)
# ──────────────────────────────────────────────
BUCKET_NAME      = os.environ.get("BRONZE_BUCKET", "ecommerce-pipeline-bronze")
FIREHOSE_STREAM  = os.environ.get("FIREHOSE_STREAM", "ecommerce-order-stream")
AWS_REGION       = os.environ.get("AWS_REGION", "us-east-1")

CATEGORIES = ["Electronics", "Clothing", "Books", "Home & Kitchen",
              "Sports", "Toys", "Beauty", "Grocery", "Automotive"]

STATUSES   = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
STATUS_W   = [0.05, 0.10, 0.15, 0.55, 0.10, 0.05]

SEGMENTS   = ["Bronze", "Silver", "Gold", "Platinum"]
COUNTRIES  = ["US", "UK", "CA", "AU", "DE", "FR", "IN", "JP", "BR", "MX"]

# ──────────────────────────────────────────────
# GENERATORS
# ──────────────────────────────────────────────

def make_customers(n: int) -> list[dict]:
    customers = []
    for _ in range(n):
        signup = fake.date_time_between(start_date="-2y", end_date="now")
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "name":        fake.name(),
            "email":       fake.unique.email(),
            "country":     random.choice(COUNTRIES),
            "signup_date": signup.isoformat(),
            "segment":     random.choices(SEGMENTS, weights=[40, 30, 20, 10])[0],
            "phone":       fake.phone_number(),
            "city":        fake.city(),
        })
    return customers


def make_products(n: int) -> list[dict]:
    products = []
    for _ in range(n):
        base_price = round(random.uniform(5.0, 999.99), 2)
        products.append({
            "product_id":  str(uuid.uuid4()),
            "name":        fake.catch_phrase(),
            "category":    random.choice(CATEGORIES),
            "price":       base_price,
            "cost":        round(base_price * random.uniform(0.3, 0.7), 2),
            "stock_qty":   random.randint(0, 500),
            "supplier":    fake.company(),
            "sku":         fake.bothify("??-####-??").upper(),
            "rating":      round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(0, 2000),
        })
    return products


def make_orders(n: int, customers: list[dict], products: list[dict]) -> tuple[list, list]:
    orders      = []
    order_items = []

    for _ in range(n):
        customer = random.choice(customers)
        order_ts = fake.date_time_between(start_date="-90d", end_date="now")
        order_id = str(uuid.uuid4())
        n_items  = random.randint(1, 6)

        items_total = 0.0
        for _ in range(n_items):
            product  = random.choice(products)
            qty      = random.randint(1, 5)
            discount = round(random.choices([0, 0.05, 0.10, 0.15, 0.20],
                                            weights=[60, 15, 10, 10, 5])[0], 2)
            unit_price = product["price"]
            line_total = round(qty * unit_price * (1 - discount), 2)
            items_total += line_total

            order_items.append({
                "item_id":    str(uuid.uuid4()),
                "order_id":   order_id,
                "product_id": product["product_id"],
                "quantity":   qty,
                "unit_price": unit_price,
                "discount":   discount,
                "line_total": line_total,
                "created_at": order_ts.isoformat(),
            })

        shipping = round(random.choice([0, 4.99, 9.99, 14.99]), 2)
        orders.append({
            "order_id":    order_id,
            "customer_id": customer["customer_id"],
            "status":      random.choices(STATUSES, weights=STATUS_W)[0],
            "subtotal":    round(items_total, 2),
            "shipping":    shipping,
            "total":       round(items_total + shipping, 2),
            "currency":    "USD",
            "created_at":  order_ts.isoformat(),
            "updated_at":  (order_ts + timedelta(hours=random.randint(1, 72))).isoformat(),
            "channel":     random.choice(["web", "mobile", "api"]),
            "promo_code":  fake.bothify("PROMO-####") if random.random() < 0.2 else None,
        })

    return orders, order_items


# ──────────────────────────────────────────────
# OUTPUT HELPERS
# ──────────────────────────────────────────────

def write_csv(records: list[dict], filename: str) -> str:
    if not records:
        return filename
    os.makedirs("data/raw", exist_ok=True)
    path = f"data/raw/{filename}"
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} rows → {path}")
    return path


def upload_to_s3(local_path: str, s3_key: str):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(local_path, BUCKET_NAME, s3_key)
    print(f"  Uploaded → s3://{BUCKET_NAME}/{s3_key}")


def send_to_firehose(records: list[dict], batch_size: int = 500):
    firehose = boto3.client("firehose", region_name=AWS_REGION)
    total_sent = 0

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        firehose_records = [
            {"Data": (json.dumps(rec) + "\n").encode("utf-8")}
            for rec in batch
        ]
        resp = firehose.put_record_batch(
            DeliveryStreamName=FIREHOSE_STREAM,
            Records=firehose_records,
        )
        failed = resp.get("FailedPutCount", 0)
        total_sent += len(batch) - failed
        print(f"  Sent batch {i//batch_size + 1}: {len(batch)-failed} records ({failed} failed)")
        time.sleep(0.1)   # respect Firehose throttle

    print(f"  Total sent to Firehose: {total_sent:,}")


# ──────────────────────────────────────────────
# MODES
# ──────────────────────────────────────────────

def run_batch(n_orders: int):
    print("\n🚀 Batch mode — generating data …")
    customers = make_customers(max(n_orders // 10, 100))
    products  = make_products(max(n_orders // 20, 50))
    orders, order_items = make_orders(n_orders, customers, products)

    today = datetime.utcnow().strftime("%Y/%m/%d")

    for name, data in [
        ("customers", customers),
        ("products",  products),
        ("orders",    orders),
        ("order_items", order_items),
    ]:
        path   = write_csv(data, f"{name}.csv")
        s3_key = f"bronze/{name}/dt={today}/{name}.csv"
        upload_to_s3(path, s3_key)

    print("\nBatch upload complete.")


def run_stream(n_events: int):
    print(f"\n🚀 Stream mode — sending {n_events:,} events to Firehose …")
    customers = make_customers(max(n_events // 10, 50))
    products  = make_products(max(n_events // 20, 30))
    orders, _ = make_orders(n_events, customers, products)

    send_to_firehose(orders)
    print("\nStreaming complete.")


# ──────────────────────────────────────────────
# ENTRYPOINT
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce Data Generator")
    parser.add_argument("--mode",   choices=["batch", "stream"], default="batch",
                        help="'batch' uploads CSVs to S3; 'stream' sends to Kinesis Firehose")
    parser.add_argument("--events", type=int, default=1000,
                        help="Number of orders/events to generate")
    parser.add_argument("--rows",   type=int, default=5000,
                        help="Number of rows per table in batch mode")
    args = parser.parse_args()

    if args.mode == "batch":
        run_batch(args.rows)
    else:
        run_stream(args.events)
