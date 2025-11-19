import json
import os
import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

dynamodb = boto3.client("dynamodb")
stepfunctions = boto3.client("stepfunctions")


@logger.inject_lambda_context(log_event=True)
def handle_bedrock_task_complete(event: dict, context: LambdaContext):
    """
    Handle Bedrock batch job completion events from EventBridge.
    Extract job name from the event and resume the Step Functions workflow.
    """
    logger.info("Received Bedrock completion event", extra={"event": event})

    # Extract job name from EventBridge event
    detail = event.get("detail", {})
    job_name = detail.get("batchJobName")

    if not job_name:
        job_name = event.get("batchJobName")

    if not job_name:
        raise Exception("No job name found in event detail")

    logger.info(f"Processing completion for Bedrock batch job: {job_name}")

    # Get task token from DynamoDB
    table_name = os.environ["DYNAMODB_TABLE"]
    response = dynamodb.get_item(TableName=table_name, Key={"JobId": {"S": job_name}})

    if "Item" not in response:
        raise Exception(f"No task token found for job: {job_name}")

    task_token = response["Item"]["TaskToken"]["S"]
    logger.info(f"Found task token for job: {job_name}")

    # Send success to Step Functions
    stepfunctions.send_task_success(
        taskToken=task_token,
        output=json.dumps({"jobName": job_name, "status": "completed"}),
    )

    logger.info(f"Successfully resumed Step Functions for job: {job_name}")
