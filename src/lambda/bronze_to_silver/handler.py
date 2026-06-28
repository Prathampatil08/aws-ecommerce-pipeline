
import boto3, csv, io, json, os
from datetime import datetime

s3 = boto3.client("s3")
SILVER_BUCKET = os.environ["SILVER_BUCKET"]

def clean_row(row, table):
    if table == "orders":
        row["status"]   = row.get("status","").lower().strip()
        row["currency"] = row.get("currency","").upper().strip()
        row["channel"]  = row.get("channel","").lower().strip()
        try:
            row["total"] = str(abs(float(row.get("total",0))))
        except:
            row["total"] = "0"
    if table == "customers":
        row["email"]   = row.get("email","").lower().strip()
        row["country"] = row.get("country","").upper().strip()
    if table == "products":
        row["category"] = row.get("category","").strip().title()
        row["sku"]      = row.get("sku","").upper().strip()
    return row

def handler(event, context):
    bucket = event["bucket"]
    key    = event["key"]
    table  = event["table"]
    date   = event["date"]

    obj  = s3.get_object(Bucket=bucket, Key=key)
    text = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))
    rows = [clean_row(r, table) for r in reader if any(v.strip() for v in r.values())]

    seen = set()
    deduped = []
    pk = {"orders":"order_id","customers":"customer_id","products":"product_id","order_items":"item_id"}.get(table,"id")
    for row in rows:
        key_val = row.get(pk,"")
        if key_val and key_val not in seen:
            seen.add(key_val)
            deduped.append(row)

    if not deduped:
        return {"rows": 0}

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=deduped[0].keys())
    writer.writeheader()
    writer.writerows(deduped)

    s3_key = f"silver/{table}/dt={date}/{table}_clean.csv"
    s3.put_object(Bucket=SILVER_BUCKET, Key=s3_key, Body=out.getvalue().encode())
    print(f"Wrote {len(deduped)} rows to s3://{SILVER_BUCKET}/{s3_key}")
    return {"rows": len(deduped), "key": s3_key}
