
import boto3, json, os
from datetime import datetime, timedelta

sfn = boto3.client("stepfunctions")
SFN_ARN = os.environ.get("SFN_ARN","")

def handler(event, context):
    date = event.get("date", (datetime.utcnow()-timedelta(days=1)).strftime("%Y-%m-%d"))
    if SFN_ARN:
        resp = sfn.start_execution(
            stateMachineArn=SFN_ARN,
            name=f"pipeline-{date}-{datetime.utcnow().strftime("%H%M%S")}",
            input=json.dumps({"date": date})
        )
        return {"execution": resp["executionArn"]}
    return {"date": date}
