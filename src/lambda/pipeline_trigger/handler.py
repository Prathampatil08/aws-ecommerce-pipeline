"""
Lambda: pipeline_trigger
Invoked on a schedule (EventBridge) to run the daily pipeline,
or manually to trigger a backfill for a given date range.

Event payload examples:
  {}                                      → runs for yesterday
  {"processing_date": "2024-11-01"}       → runs for specific date
  {"start_date": "2024-10-01",
   "end_date":   "2024-10-31"}            → backfill range
"""

import json
import os
import boto3
import logging
from datetime import datetime, timedelta, date

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SFN_ARN       = os.environ["STEP_FUNCTIONS_ARN"]
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "ecommerce-pipeline")
MAX_BACKFILL  = int(os.environ.get("MAX_BACKFILL_DAYS", "30"))

sfn = boto3.client("stepfunctions")


def start_execution(processing_date: str, mode: str = "daily") -> dict:
    exec_name = f"{PIPELINE_NAME}-{processing_date.replace('-', '')}-{mode}"

    try:
        resp = sfn.start_execution(
            stateMachineArn=SFN_ARN,
            name=exec_name[:80],
            input=json.dumps({
                "processing_date": processing_date,
                "mode":            mode,
                "triggered_at":    datetime.utcnow().isoformat(),
            })
        )
        logger.info(f"Started execution: {resp['executionArn']}")
        return {"date": processing_date, "execution_arn": resp["executionArn"], "status": "STARTED"}

    except sfn.exceptions.ExecutionAlreadyExists:
        logger.info(f"Execution {exec_name} already exists — skipped")
        return {"date": processing_date, "status": "SKIPPED_ALREADY_EXISTS"}

    except Exception as e:
        logger.error(f"Failed to start execution for {processing_date}: {e}")
        return {"date": processing_date, "status": "FAILED", "error": str(e)}


def handler(event, context):
    logger.info(f"Trigger event: {json.dumps(event)}")

    # ── Single date ──
    if "processing_date" in event:
        result = start_execution(event["processing_date"], mode="manual")
        return {"statusCode": 200, "body": result}

    # ── Date range (backfill) ──
    if "start_date" in event and "end_date" in event:
        start  = datetime.strptime(event["start_date"], "%Y-%m-%d").date()
        end    = datetime.strptime(event["end_date"],   "%Y-%m-%d").date()
        n_days = (end - start).days + 1

        if n_days > MAX_BACKFILL:
            return {
                "statusCode": 400,
                "body": f"Backfill range {n_days} days exceeds MAX_BACKFILL_DAYS={MAX_BACKFILL}"
            }

        results = []
        current = start
        while current <= end:
            results.append(start_execution(current.strftime("%Y-%m-%d"), mode="backfill"))
            current += timedelta(days=1)

        return {"statusCode": 200, "body": results}

    # ── Default: yesterday ──
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    result = start_execution(yesterday, mode="scheduled")
    return {"statusCode": 200, "body": result}
