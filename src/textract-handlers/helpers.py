import boto3
from aws_lambda_powertools import Logger

# This is a general logger to import and reuse
logger = Logger()


def get_text_as_paragraphs(job_id: str) -> str:
    """
    Gets all text from a Textract job and formats it into paragraphs.

    Args:
        job_id: The ID of the Textract job

    Returns:
        String containing all detected text with line breaks
    """
    textract = boto3.client("textract")

    blocks = []
    page = 1
    logger.info(f"Fetching Textract results for page {page}")
    response = textract.get_document_analysis(JobId=job_id)
    blocks.extend(response["Blocks"])

    while "NextToken" in response:
        logger.info(f"Fetching Textract results for page {page}")
        response = textract.get_document_analysis(JobId=job_id, NextToken=response["NextToken"])
        blocks.extend(response["Blocks"])
        page += 1

    # Sort blocks and build text
    sorted_blocks = _sort_blocks(blocks)
    return "\n".join(block["Text"] for block in sorted_blocks)


def _sort_blocks(blocks):
    """
    Sorts blocks by column and vertical position
    """

    pages = {}
    for block in blocks:
        if block["BlockType"] == "LINE":
            page_num = block.get("Page", 1)
            if page_num not in pages:
                pages[page_num] = []
            pages[page_num].append(block)

    # Sort each page's blocks
    result = []
    for page_num in sorted(pages.keys()):
        page_blocks = pages[page_num]
        # Group by column
        columns = {}
        for block in page_blocks:
            col_idx = _get_column_index(block)
            if col_idx not in columns:
                columns[col_idx] = []
            columns[col_idx].append(block)

        # Sort each column by vertical position
        for col_idx in sorted(columns.keys()):
            columns[col_idx].sort(key=lambda b: b["Geometry"]["BoundingBox"]["Top"])
            result.extend(columns[col_idx])

    return result


def _get_column_index(block):
    """
    Determines which column a block belongs to based on its X coordinate
    """
    bbox = block["Geometry"]["BoundingBox"]
    x_center = bbox["Left"] + (bbox["Width"] / 2)
    # Assuming a two-column layout with middle point at 0.5
    return 0 if x_center < 0.5 else 1
