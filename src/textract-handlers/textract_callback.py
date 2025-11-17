import json
import os

import boto3
from aws_lambda_powertools.utilities.data_classes import SNSEvent, event_source

from helpers import logger

TABLE_NAME = os.environ["DYNAMODB_TABLE"]

dynamodb = boto3.resource("dynamodb")


@logger.inject_lambda_context
@event_source(data_class=SNSEvent)
def handle_textract_task_complete(event, context):
    # Multiple records can be delivered in a single event
    for record in event.records:
        sns_message = json.loads(record.sns.message)
        textract_job_id = sns_message["JobId"]
        logger.info(f"Sending task token for textract job {textract_job_id}.")

        # Get both task token and original file from DynamoDB
        ddb_item = _get_item_from_ddb(textract_job_id)

        # Send both the job ID and original file name in the response
        _send_task_success(
            ddb_item["TaskToken"],
            {
                "TextractJobId": textract_job_id,
                "OriginalFile": ddb_item["OriginalFile"],
            },
        )
        # Delete the task token from DynamoDB after use
        _delete_item_from_ddb(textract_job_id)


def _send_task_success(task_token: str, output: None | dict = None) -> None:
    """Sends task success to Step Functions with the provided output"""
    sfn = boto3.client("stepfunctions")
    sfn.send_task_success(taskToken=task_token, output=json.dumps(output or {}))


def _get_item_from_ddb(textract_job_id):
    """Retrieves the full item from DynamoDB using the Textract JobId as key"""
    table = dynamodb.Table(TABLE_NAME)
    response = table.get_item(Key={"JobId": textract_job_id})
    return response.get("Item")


def _delete_item_from_ddb(textract_job_id):
    """Deletes the item from DynamoDB using the Textract JobId as key"""
    table = dynamodb.Table(TABLE_NAME)
    table.delete_item(Key={"JobId": textract_job_id})
