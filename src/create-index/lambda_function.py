from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import os
import boto3
import json
import logging
import cfnresponse
import time

HOST = os.environ.get("COLLECTION_HOST")
VECTOR_INDEX_NAME = os.environ.get("VECTOR_INDEX_NAME")
VECTOR_FIELD_NAME = os.environ.get("VECTOR_FIELD_NAME")
REGION_NAME = os.environ.get("REGION_NAME")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def log(message):
    logger.info(message)


def lambda_handler(event, context):
    """
    Lambda handler to create OpenSearch Index
    """
    log(f"Event: {json.dumps(event)}")

    session = boto3.Session()
    creds = session.get_credentials()

    # Get STS client from session
    sts_client = session.client("sts")

    # Get caller identity
    caller_identity = sts_client.get_caller_identity()

    # Print the caller identity information
    log(f"Caller Identity: {caller_identity}")
    log(f"ARN: {caller_identity['Arn']}")
    log(f"HOST: {HOST}")

    host = HOST.split("//")[1]

    region = REGION_NAME
    service = "aoss"
    status = cfnresponse.SUCCESS
    response = {}

    try:
        auth = AWSV4SignerAuth(creds, region, service)

        client = OpenSearch(
            hosts=[{"host": host, "port": 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20,
        )
        index_name = VECTOR_INDEX_NAME

        if event["RequestType"] == "Create":
            log(f"Creating index: {index_name}")

            index_body = {
                "settings": {
                    "index.knn": True,
                    "index.knn.algo_param.ef_search": 512,
                },
                "mappings": {
                    "properties": {
                        VECTOR_FIELD_NAME: {
                            "type": "knn_vector",
                            "dimension": 1024,
                            "method": {
                                "space_type": "innerproduct",
                                "engine": "FAISS",
                                "name": "hnsw",
                                "parameters": {
                                    "m": 16,
                                    "ef_construction": 512,
                                },
                            },
                        },
                        "AMAZON_BEDROCK_METADATA": {"type": "text", "index": False},
                        "AMAZON_BEDROCK_TEXT_CHUNK": {"type": "text"},
                        "id": {"type": "text"},
                    }
                },
            }

            response = client.indices.create(index_name, body=index_body)

            log(f"Response: {response}")

            log("Sleeping for 1 minutes to let index create.")
            # nosemgrep: arbitrary-sleep
            time.sleep(60)

        elif event["RequestType"] == "Delete":
            log(f"Deleting index: {index_name}")
            response = client.indices.delete(index_name)
            log(f"Response: {response}")
        else:
            log("Continuing without action.")

    except Exception as e:
        logging.error("Exception: %s" % e, exc_info=True)
        status = cfnresponse.FAILED

    finally:
        cfnresponse.send(event, context, status, response)

    return {
        "statusCode": 200,
        "body": json.dumps("Create index lambda ran successfully."),
    }
