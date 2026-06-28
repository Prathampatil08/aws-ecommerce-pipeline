import os

# ── 1. Bronze to Silver Lambda ──────────────────
os.makedirs('src/lambda/bronze_to_silver', exist_ok=True)
open('src/lambda/bronze_to_silver/handler.py', 'w').write('''
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
''')

# ── 2. Silver to Gold Lambda ─────────────────────
os.makedirs('src/lambda/silver_to_gold', exist_ok=True)
open('src/lambda/silver_to_gold/handler.py', 'w').write('''
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
''')

# ── 3. Pipeline Orchestrator Lambda ──────────────
os.makedirs('src/lambda/pipeline_orchestrator', exist_ok=True)
open('src/lambda/pipeline_orchestrator/handler.py', 'w').write('''
import boto3, json, os
from datetime import datetime, timedelta

sfn = boto3.client("stepfunctions")
SFN_ARN = os.environ.get("SFN_ARN","")

def handler(event, context):
    date = event.get("date", (datetime.utcnow()-timedelta(days=1)).strftime("%Y-%m-%d"))
    if SFN_ARN:
        resp = sfn.start_execution(
            stateMachineArn=SFN_ARN,
            name=f"pipeline-{date}-{datetime.utcnow().strftime(\"%H%M%S\")}",
            input=json.dumps({"date": date})
        )
        return {"execution": resp["executionArn"]}
    return {"date": date}
''')

# ── 4. New CloudFormation Template ───────────────
template = """AWSTemplateFormatVersion: "2010-09-09"
Description: E-Commerce Data Pipeline (Lambda-based ETL)

Parameters:
  ProjectName:
    Type: String
    Default: ecommerce-pipeline
  ScriptsBucket:
    Type: String

Resources:

  BronzeBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${ProjectName}-bronze-${AWS::AccountId}"
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  SilverBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${ProjectName}-silver-${AWS::AccountId}"
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  GoldBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${ProjectName}-gold-${AWS::AccountId}"
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  AthenaResultsBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: !Sub "${ProjectName}-athena-${AWS::AccountId}"
      PublicAccessBlockConfiguration:
        BlockPublicAcls: true
        BlockPublicPolicy: true
        IgnorePublicAcls: true
        RestrictPublicBuckets: true

  LambdaRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${ProjectName}-lambda-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: lambda.amazonaws.com
            Action: sts:AssumeRole
      ManagedPolicyArns:
        - arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
      Policies:
        - PolicyName: S3Access
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action: ["s3:*"]
                Resource: ["*"]
              - Effect: Allow
                Action: ["states:StartExecution"]
                Resource: ["*"]

  StepFunctionsRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub "${ProjectName}-sfn-role"
      AssumeRolePolicyDocument:
        Version: "2012-10-17"
        Statement:
          - Effect: Allow
            Principal:
              Service: states.amazonaws.com
            Action: sts:AssumeRole
      Policies:
        - PolicyName: InvokeLambda
          PolicyDocument:
            Version: "2012-10-17"
            Statement:
              - Effect: Allow
                Action: ["lambda:InvokeFunction"]
                Resource: ["*"]

  BronzeToSilverLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub "${ProjectName}-bronze-to-silver"
      Runtime: python3.11
      Handler: handler.handler
      Role: !GetAtt LambdaRole.Arn
      Timeout: 300
      MemorySize: 512
      Code:
        S3Bucket: !Ref ScriptsBucket
        S3Key: lambda/bronze_to_silver.zip
      Environment:
        Variables:
          SILVER_BUCKET: !Ref SilverBucket

  SilverToGoldLambda:
    Type: AWS::Lambda::Function
    Properties:
      FunctionName: !Sub "${ProjectName}-silver-to-gold"
      Runtime: python3.11
      Handler: handler.handler
      Role: !GetAtt LambdaRole.Arn
      Timeout: 300
      MemorySize: 512
      Code:
        S3Bucket: !Ref ScriptsBucket
        S3Key: lambda/silver_to_gold.zip
      Environment:
        Variables:
          SILVER_BUCKET: !Ref SilverBucket
          GOLD_BUCKET: !Ref GoldBucket

  PipelineStateMachine:
    Type: AWS::StepFunctions::StateMachine
    Properties:
      StateMachineName: !Sub "${ProjectName}-pipeline"
      RoleArn: !GetAtt StepFunctionsRole.Arn
      DefinitionString: !Sub |
        {
          "Comment": "E-Commerce ETL Pipeline",
          "StartAt": "BronzeToSilver",
          "States": {
            "BronzeToSilver": {
              "Type": "Task",
              "Resource": "${BronzeToSilverLambda.Arn}",
              "Parameters": {
                "bucket.$": "$.bucket",
                "key.$": "$.key",
                "table.$": "$.table",
                "date.$": "$.date"
              },
              "ResultPath": "$.bronze_result",
              "Next": "SilverToGold",
              "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2}]
            },
            "SilverToGold": {
              "Type": "Task",
              "Resource": "${SilverToGoldLambda.Arn}",
              "Parameters": {
                "date.$": "$.date"
              },
              "ResultPath": "$.gold_result",
              "Next": "Done",
              "Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2}]
            },
            "Done": {
              "Type": "Succeed"
            }
          }
        }

  AthenaWorkgroup:
    Type: AWS::Athena::WorkGroup
    Properties:
      Name: !Sub "${ProjectName}-workgroup"
      State: ENABLED
      WorkGroupConfiguration:
        ResultConfiguration:
          OutputLocation: !Sub "s3://${AthenaResultsBucket}/results/"

Outputs:
  BronzeBucket:
    Value: !Ref BronzeBucket
  SilverBucket:
    Value: !Ref SilverBucket
  GoldBucket:
    Value: !Ref GoldBucket
  StateMachineArn:
    Value: !Ref PipelineStateMachine
"""

open('infrastructure/cloudformation/main-stack.yaml', 'w').write(template)
print('All files rebuilt successfully')
