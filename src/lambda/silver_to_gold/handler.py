
import boto3, csv, io, os
from collections import defaultdict

s3 = boto3.client("s3")
SILVER_BUCKET = os.environ["SILVER_BUCKET"]
GOLD_BUCKET   = os.environ["GOLD_BUCKET"]

def read_csv_from_s3(bucket, prefix):
    rows = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode()
            reader = csv.DictReader(io.StringIO(body))
            rows.extend(list(reader))
    return rows

def write_csv_to_s3(bucket, key, rows):
    if not rows: return
    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    s3.put_object(Bucket=bucket, Key=key, Body=out.getvalue().encode())
    print(f"Wrote {len(rows)} rows to s3://{bucket}/{key}")

def handler(event, context):
    date = event["date"]

    orders   = read_csv_from_s3(SILVER_BUCKET, "silver/orders/")
    customers = read_csv_from_s3(SILVER_BUCKET, "silver/customers/")

    # Daily revenue summary
    revenue = defaultdict(lambda: {"total_orders":0,"gross_revenue":0.0,"total_units":0})
    for o in orders:
        if o.get("status") == "cancelled": continue
        d = o.get("created_at","")[:10]
        ch = o.get("channel","unknown")
        k = f"{d}|{ch}"
        revenue[k]["order_date"] = d
        revenue[k]["channel"]    = ch
        revenue[k]["total_orders"] += 1
        try:
            revenue[k]["gross_revenue"] += float(o.get("total",0))
        except: pass

    daily_rows = []
    for k, v in revenue.items():
        v["gross_revenue"] = round(v["gross_revenue"], 2)
        if v["total_orders"] > 0:
            v["aov"] = round(v["gross_revenue"] / v["total_orders"], 2)
        daily_rows.append(v)

    write_csv_to_s3(GOLD_BUCKET, f"gold/daily_revenue/dt={date}/summary.csv", daily_rows)

    # Customer country breakdown
    cust_map = {c["customer_id"]: c.get("country","Unknown") for c in customers}
    geo = defaultdict(lambda: {"revenue":0.0,"orders":0})
    for o in orders:
        if o.get("status") == "cancelled": continue
        country = cust_map.get(o.get("customer_id",""), "Unknown")
        geo[country]["country"] = country
        geo[country]["orders"]  += 1
        try:
            geo[country]["revenue"] += float(o.get("total",0))
        except: pass

    geo_rows = [{"country":k,"orders":v["orders"],"revenue":round(v["revenue"],2)} for k,v in geo.items()]
    write_csv_to_s3(GOLD_BUCKET, f"gold/geo_revenue/dt={date}/summary.csv", geo_rows)

    # Order status funnel
    funnel = defaultdict(int)
    for o in orders:
        funnel[o.get("status","unknown")] += 1
    funnel_rows = [{"status":k,"order_count":v} for k,v in funnel.items()]
    write_csv_to_s3(GOLD_BUCKET, f"gold/order_funnel/dt={date}/summary.csv", funnel_rows)

    return {"status": "success", "date": date, "orders_processed": len(orders)}
