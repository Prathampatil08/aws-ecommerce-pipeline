"""
Lambda: batch_ingestion
Triggered by S3 PutObject events on the Bronze bucket.
Validates the incoming file, logs metadata to CloudWatch,
and optionally starts the Step Functions pipeline.
"""

import json
import os
import boto3
import logging
from datetime import datetime
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SFN_ARN        = os.environ.get("STEP_FUNCTIONS_ARN", "")
PIPELINE_NAME  = os.environ.get("PIPELINE_NAME", "ecommerce-pipeline")
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")
AUTO_TRIGGER   = os.environ.get("AUTO_TRIGGER_PIPELINE", "true").lower() == "true"

VALID_TABLES   = {"orders", "customers", "products", "order_items"}
VALID_EXTS     = {".csv", ".json", ".parquet"}

s3  = boto3.client("s3",           region_name=AWS_REGION)
sfn = boto3.client("stepfunctions", region_name=AWS_REGION)
cw  = boto3.client("cloudwatch",   region_name=AWS_REGION)


def parse_s3_key(key: str) -> dict:
    """
    Expected key pattern:
      bronze/{table}/dt={YYYY/MM/DD}/{filename}.csv
    Returns dict with table, date, filename, ext.
    """
    parts = key.split("/")
    result = {"table": None, "date": None, "filename": None, "ext": None}
    if len(parts) >= 4 and parts[0] == "bronze":
        result["table"]    = parts[1]
        result["date"]     = parts[2].replace("dt=", "").replace("/", "-")
        result["filename"] = parts[-1]
        result["ext"]      = "." + parts[-1].rsplit(".", 1)[-1] if "." in parts[-1] else ""
    return result


def put_metric(metric_name: str, value: float, unit: str = "Count", table: str = "unknown"):
    try:
        cw.put_metric_data(
            Namespace="EcommercePipeline",
            MetricData=[{
                "MetricName": metric_name,
                "Dimensions": [{"Name": "Table", "Value": table}],
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.utcnow(),
            }]
        )
    except Exception as e:
        logger.warning(f"CloudWatch metric failed: {e}")


def start_pipeline(processing_date: str, execution_name: str):
    if not SFN_ARN:
        logger.info("SFN_ARN not set — skipping pipeline trigger")
        return None
    try:
        resp = sfn.start_execution(
            stateMachineArn=SFN_ARN,
            name=execution_name[:80],   # SFN max name length
            input=json.dumps({
                "processing_date": processing_date,
                "triggered_by":    "s3_event",
                "triggered_at":    datetime.utcnow().isoformat(),
            })
        )
        logger.info(f"Step Functions started: {resp['executionArn']}")
        return resp["executionArn"]
    except sfn.exceptions.ExecutionAlreadyExists:
        logger.info(f"Execution {execution_name} already running — skipped")
        return None


def get_file_metadata(bucket: str, key: str) -> dict:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return {
            "size_bytes":    head["ContentLength"],
            "last_modified": head["LastModified"].isoformat(),
            "etag":          head["ETag"],
        }
    except Exception as e:
        logger.warning(f"Could not get metadata for {key}: {e}")
        return {}


def handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")

    processed = []
    errors    = []

    for record in event.get("Records", []):
        try:
            bucket = record["s3"]["bucket"]["name"]
            key    = unquote_plus(record["s3"]["object"]["key"])

            logger.info(f"Processing: s3://{bucket}/{key}")

            # Parse key
            meta = parse_s3_key(key)
            if meta["table"] is None:
                logger.warning(f"Unrecognised key pattern: {key} — skipping")
                continue

            # Validate
            if meta["table"] not in VALID_TABLES:
                logger.warning(f"Unknown table '{meta['table']}' in key: {key}")
                put_metric("UnknownTable", 1)
                continue

            if meta["ext"] not in VALID_EXTS:
                logger.warning(f"Unsupported extension '{meta['ext']}' in key: {key}")
                continue

            # File metadata
            file_meta = get_file_metadata(bucket, key)
            size_mb   = file_meta.get("size_bytes", 0) / 1024 / 1024

            logger.info(f"  table={meta['table']} date={meta['date']} "
                        f"size={size_mb:.2f}MB")

            # CloudWatch metrics
            put_metric("FilesIngested", 1,          table=meta["table"])
            put_metric("BytesIngested", size_mb,    table=meta["table"], unit="Megabytes")

            result = {
                "bucket":   bucket,
                "key":      key,
                "table":    meta["table"],
                "date":     meta["date"],
                "size_mb":  round(size_mb, 3),
                **file_meta,
            }

            # Trigger pipeline (once per processing date)
            if AUTO_TRIGGER and meta["date"]:
                exec_name = f"{PIPELINE_NAME}-{meta['date'].replace('-', '')}-auto"
                arn = start_pipeline(meta["date"], exec_name)
                result["pipeline_execution"] = arn

            processed.append(result)

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            put_metric("IngestionErrors", 1)
            errors.append(str(e))

    response = {
        "statusCode": 200 if not errors else 207,
        "body": {
            "processed": processed,
            "errors":    errors,
            "summary":   {
                "total_records":    len(event.get("Records", [])),
                "processed_count":  len(processed),
                "error_count":      len(errors),
            }
        }
    }
    logger.info(f"Response: {json.dumps(response)}")
    return response
