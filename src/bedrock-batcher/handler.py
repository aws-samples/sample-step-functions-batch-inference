import json
import os
import uuid
from dataclasses import dataclass
from io import StringIO
from typing import List

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
BEDROCK_BATCH_RESULTS_PREFIX = os.environ["BEDROCK_BATCH_RESULTS_PREFIX"]

JSON_FORMAT = json.dumps(
    {
        "has_code": False,
        "has_dataset": False,
        "code_availability": "not_available",
        "dataset_availability": "not_available",
        "code_repository_url": "",
        "research_type": "empirical_study",
        "is_reproducible": False,
    }
)

EXAMPLE_RESPONSE = json.dumps(
    {
        "has_code": True,
        "has_dataset": True,
        "code_availability": "publicly_available",
        "dataset_availability": "available_on_request",
        "code_repository_url": "https://github.com/google-research/vision_transformer",
        "research_type": "methodology",
        "is_reproducible": True,
    }
)

PROMPT_TEMPLATE = """
Analyze the following research paper transcript and extract metadata about code and dataset availability.

Extract the following metadata from this research paper transcript:

1. **has_code**: Does the paper mention or link to source code? (true/false)
2. **has_dataset**: Does the paper introduce, use, or provide a dataset? (true/false)
3. **code_availability**: If has_code is true, how is the code available?
   - "publicly_available": Code is available on GitHub, GitLab, or other public repositories
   - "available_on_request": Code available by contacting authors
   - "private_or_proprietary": Code exists but is not publicly available
   - "not_available": No code availability mentioned
4. **dataset_availability**: If has_dataset is true, how is the dataset available? (same options as above)
5. **code_repository_url**: If available, provide the URL to the code repository (empty string if none)
6. **research_type**: What type of research is this?
   - "methodology": Introduces new methods, algorithms, or techniques
   - "theoretical": Theoretical analysis, proofs, or mathematical frameworks
   - "empirical_study": Experimental validation, comparative studies, or empirical analysis
   - "survey_or_review": Literature review or survey paper
   - "case_study": Specific application or case study
7. **is_reproducible**: Based on available code, data, and methodology, are results likely reproducible? (true/false)

Look for explicit mentions of:
- GitHub, GitLab, or other repository URLs
- "Code is available at...", "Implementation can be found...", "Source code..."
- Dataset names, data collection methods, or data availability statements
- Experimental setup details that would enable reproduction

Paper transcript follows:

<paper_text>
{research_paper}
</paper_text>

You must provide your response in the following JSON format:
{output_format}

Here is an example response:
{example_response}

Return only valid JSON matching the schema above. Do not include any text outside of the JSON structure."""


@dataclass
class TextractResult:
    text: str
    job_id: str
    original_file: str
    s3_bucket: str
    s3_key: str


@logger.inject_lambda_context
def create_bedrock_batch_file(event: dict, context: LambdaContext):
    """Handle Textract results and generate batch requests for Bedrock"""

    fh = StringIO()

    for result in _generate_textract_results_from_s3(event["files"]):
        logger.info(
            f"Processing {result.original_file}",
            extra={"job_id": result.job_id, "text_length": len(result.text)},
        )

        # nosemgrep: tainted-html-string
        prompt = PROMPT_TEMPLATE.format(
            research_paper=result.text,
            output_format=JSON_FORMAT,
            example_response=EXAMPLE_RESPONSE,
        )

        # Create the request body with recordId structure
        request_body = {
            "recordId": f"{result.job_id}",
            "modelInput": {
                "messages": [
                    {
                        "role": "user",
                        "content": [{"text": prompt}],
                    }
                ],
                "inferenceConfig": {"maxTokens": 4096},
            },
        }
        # Write to JSONL file
        json.dump(request_body, fh)
        fh.write("\n")

    # Get the full content as a string
    content = fh.getvalue()
    fh.close()

    job_name = uuid.uuid4().hex
    results_file_key = f"batch/{job_name}.jsonl"

    _upload_to_s3(content, results_file_key)

    return {
        "input_s3_uri": f"s3://{OUTPUT_BUCKET}/{results_file_key}",
        "output_s3_uri": f"s3://{OUTPUT_BUCKET}/{BEDROCK_BATCH_RESULTS_PREFIX}/",
        "job_name": job_name,
    }


def _upload_to_s3(content: str, key: str):
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=OUTPUT_BUCKET,
        Key=key,
        Body=content.encode("utf-8"),
    )


def _generate_textract_results_from_s3(files: List[dict]):
    """Generator function that yields TextractResult objects with loaded text content"""
    s3_client = boto3.client("s3")

    for file_info in files:
        # Get the S3 object content
        response = s3_client.get_object(
            Bucket=file_info["S3Object"]["Bucket"], Key=file_info["S3Object"]["Key"]
        )
        content = response["Body"].read().decode("utf-8")

        # Yield a TextractResult object
        yield TextractResult(
            text=content,
            job_id=file_info["TextractJobId"],
            original_file=file_info["OriginalFile"],
            s3_bucket=file_info["S3Object"]["Bucket"],
            s3_key=file_info["S3Object"]["Key"],
        )
