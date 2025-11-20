import json
import os
import re
from dataclasses import dataclass
from typing import Dict, List, Any, Optional

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

INPUT_BUCKET = os.environ["INPUT_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
BEDROCK_BATCH_RESULTS_PREFIX = os.environ["BEDROCK_BATCH_RESULTS_PREFIX"]
SNS_NOTIFICATION_TOPIC = os.environ["SNS_NOTIFICATION_TOPIC"]

logger = Logger()


@dataclass
class PaperMetadata:
    """
    Metadata schema for research papers extracted from transcripts.
    """

    has_code: bool = False
    has_dataset: bool = False
    code_availability: Optional[str] = None
    dataset_availability: Optional[str] = None
    code_repository_url: Optional[str] = None
    research_type: str = "empirical_study"
    is_reproducible: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "PaperMetadata":
        """Create PaperMetadata from dictionary, filtering valid fields."""
        valid_fields = cls.__dataclass_fields__.keys()
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)

    def to_metadata(self) -> Dict[str, Any]:
        """Convert to Bedrock Knowledge Base metadata.json format."""
        metadata = {
            "metadataAttributes": {
                "has_code": self.has_code,
                "has_dataset": self.has_dataset,
                "code_availability": self.code_availability or "not_available",
                "dataset_availability": self.dataset_availability or "not_available",
                "research_type": self.research_type,
                # nosemgrep: is-function-without-parentheses
                "is_reproducible": self.is_reproducible,
            }
        }

        if self.code_repository_url:
            metadata["metadataAttributes"][
                "code_repository_url"
            ] = self.code_repository_url

        return metadata


s3 = boto3.client("s3")


@logger.inject_lambda_context(log_event=True)
def extract_bedrock_results(event, context: LambdaContext):
    """Extract Bedrock batch results and create metadata.json files for knowledge base ingestion."""

    bedrock_job_id = event["jobArn"].split("/")[-1]
    jsonl_key = f"{BEDROCK_BATCH_RESULTS_PREFIX}/{bedrock_job_id}"

    (object_key, jsonl_output) = _get_bedrock_results(INPUT_BUCKET, jsonl_key)
    if not jsonl_output:
        logger.error(f"No results found for {bedrock_job_id}")
        return {"error": "No results found", "bedrock_job_id": bedrock_job_id}

    results = [json.loads(line.strip()) for line in jsonl_output.splitlines()]

    logger.info(f"Read {len(results)} lines of output")

    processed_results = []
    for result in results:
        paper_metadata = _parse_model_output(
            result["recordId"],
            result["modelOutput"]["output"]["message"]["content"][0]["text"],
        )

        # Extract the original transcript filename from recordId
        # The actual text files are named with _results suffix to match textract output
        transcript_filename = f"{result['recordId']}_results.txt"

        # Create metadata.json filename following Bedrock KB convention
        metadata_filename = f"{transcript_filename}.metadata.json"

        # Upload metadata JSON file to the same location as extracted text files
        _upload_json_to_s3(
            OUTPUT_BUCKET, metadata_filename, paper_metadata.to_metadata()
        )

        processed_record = {
            "record_id": result["recordId"],
            "transcript_file": transcript_filename,
            "metadata_file": metadata_filename,
            "metadata_key": metadata_filename,
            "has_code": paper_metadata.has_code,
            "has_dataset": paper_metadata.has_dataset,
            "research_type": paper_metadata.research_type,
        }
        logger.info("Processed transcript", extra=processed_record)
        processed_results.append(processed_record)

    # Send notification with summary
    _send_processing_notification(processed_results, bedrock_job_id)

    # Return results for Step Functions to use
    return {
        "processed_count": len(processed_results),
        "bedrock_job_id": bedrock_job_id,
    }


def _parse_model_output(record_id: str, model_output: str) -> PaperMetadata:
    """Parse the model output into a PaperMetadata object."""
    # The output has some extra newlines which cause the json to not parse correctly. Removing those
    # fixes the json parsing.
    cleaned_output = model_output.replace("\n", " ")
    # The model also, at times, will leave a trailing comma in the JSON string, which breaks the
    # decoder. Manually remove the trailing comma.
    cleaned_output = re.sub(r",\s*}", "}", cleaned_output)
    # Also remove trailing commas from arrays
    cleaned_output = re.sub(r",\s*\]", "]", cleaned_output)

    try:
        results = json.loads(cleaned_output)
        return PaperMetadata.from_dict(results)
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON for record {record_id}: {str(e)}")
        logger.error(f"Cleaned output: {cleaned_output[:500]}...")
        # Return a basic structure with the error
        return PaperMetadata(
            has_code=False,
            has_dataset=False,
            code_availability="not_available",
            dataset_availability="not_available",
            research_type="empirical_study",
            is_reproducible=False,
        )


def _get_bedrock_results(bucket: str, prefix: str) -> tuple:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        return (None, None)

    for obj in response["Contents"]:
        object_key = obj["Key"]
        if object_key.endswith(".jsonl.out"):
            r = s3.get_object(Bucket=bucket, Key=obj["Key"])
            return (object_key, r["Body"].read().decode("utf-8"))

    return (None, None)


def _upload_json_to_s3(bucket: str, key: str, data: dict):
    """Upload JSON data to S3."""
    json_content = json.dumps(data, indent=2, ensure_ascii=False)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json_content.encode("utf-8"),
        ContentType="application/json",
    )
    logger.info(f"Successfully wrote JSON to s3://{bucket}/{key}")


def _send_processing_notification(
    processed_results: List[Dict], bedrock_job_id: str
) -> None:
    """Send SNS notification with processing summary."""
    sns = boto3.client("sns")

    # Count papers with code and datasets
    papers_with_code = sum(
        1 for result in processed_results if result.get("has_code", False)
    )
    papers_with_dataset = sum(
        1 for result in processed_results if result.get("has_dataset", False)
    )

    message = f"""
Bedrock batch inference job {bedrock_job_id} has been processed successfully.

Processed {len(processed_results)} research paper transcripts:
- Papers with code: {papers_with_code}
- Papers with datasets: {papers_with_dataset}

Sample processed files:
"""

    for result in processed_results[:10]:  # Show first 10 results
        code_indicator = "📝" if result.get("has_code", False) else "  "
        dataset_indicator = "📊" if result.get("has_dataset", False) else "  "
        message += f"{code_indicator}{dataset_indicator} {result['transcript_file']} ({result['research_type']})\n"

    if len(processed_results) > 10:
        message += f"... and {len(processed_results) - 10} more transcripts\n"

    message += f"""

Metadata.json files have been created for all transcripts and are ready for knowledge base sync.
The data source sync job will be triggered next to ingest all documents into the knowledge base.
"""

    sns.publish(
        TopicArn=SNS_NOTIFICATION_TOPIC,
        Subject=f"Paper Metadata Extraction Complete - {len(processed_results)} transcripts processed",
        Message=message,
    )

    logger.info(
        f"Successfully sent processing notification for {len(processed_results)} transcripts"
    )
