import os

import boto3

from helpers import logger, get_text_as_paragraphs


OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]


@logger.inject_lambda_context
def handle_textract_results(event: dict, context):
    textract_job_id = event["TextractJobId"]
    original_file = event["OriginalFile"]
    logger.info(f"Fetching Textract results for job {textract_job_id}")
    textract_results = get_text_as_paragraphs(textract_job_id)

    key_name = f"{textract_job_id}_results.txt"
    _write_to_s3(key_name, textract_results)
    logger.info(f"Successfully wrote Textract results to S3 for job {textract_job_id}")

    return {
        "TextractJobId": textract_job_id,
        "OriginalFile": original_file,
        "S3Object": {"Bucket": OUTPUT_BUCKET, "Key": key_name},
    }


def _write_to_s3(key: str, body: str):
    s3 = boto3.client("s3")
    s3.put_object(Bucket=OUTPUT_BUCKET, Key=key, Body=body)
