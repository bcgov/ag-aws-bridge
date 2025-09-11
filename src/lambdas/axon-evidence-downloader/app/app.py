import hashlib
import json
import os
import time
from typing import Dict, Any
import boto3
from botocore.exceptions import ClientError
import urllib3
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import get_db_manager, StatusCodes

# Initialize boto3 client outside handler for reuse
ssm_client = boto3.client("ssm")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AXON Evidence Downloader Lambda function for processing a single SQS message to download a file.

    Args:
        event: SQS event containing single message
        context: Lambda context object

    Returns:
        Dict with processing results

    Args:
        event: SQS event containing messages
        context: Lambda context object

    Returns:
        Dict with processing results
    """

    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Extract request information
    request_id = context.aws_request_id

    # Get environment stage from environment variable
    env_stage = os.environ.get("ENV_STAGE", "dev-test")

    # Base context data for all log entries
    base_context = {
        "request_id": request_id,
        "function_name": context.function_name,
        "env_stage": env_stage,
    }

    try:
        # Log the start of the function
        logger.log_start(event="axon_evidence_downloader", job_id=request_id)

        records = event.get("Records", [])
        record = records[0]
        message_id = record.get("messageId")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader_message_id",
            context_data={
                "env_stage": env_stage,
                "message_id": f"Processing message id {message_id}",
            },
        )

        # Parse the message body
        message_body = json.loads(record.get("body", "{}"))
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader_message_body",
            context_data={
                "env_stage": env_stage,
                "message_body": f"Message content: {message_body}",
            },
        )

        # Retrieve SSM parameters
        ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

        evidence_id = message_body.get("evidence_id")
        job_id = message_body.get("job_id")
        evidence_file_id = message_body.get("evidence_file_id")

        # Retrieve agency
        db_manager = get_db_manager(env_param_in=env_stage)
        source_agency = db_manager.get_source_agency_for_evidence(evidence_id, job_id)
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader_source_agency",
            context_data={
                "env_stage": env_stage,
                "source_agency": f"{source_agency}",
            },
        )

        # Construct url
        base_url = ssm_parameters["base_url"]
        evidence_file_url = construct_evidence_file_url(
            base_url, source_agency, evidence_id, evidence_file_id
        )
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader_evidence_file_url",
            context_data={
                "env_stage": env_stage,
                "evidence_file_url": f"{evidence_file_url}",
            },
        )

        # Download file to /tmp
        bearer_token = ssm_parameters["bearer"]
        file_path = download_evidence_file(
            evidence_file_url, evidence_file_id, bearer_token
        )

        # Calculate checksum
        file_checksum = calculate_file_checksum(file_path)

        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "message": f"Successfully processed message {message_id}",
            },
        )

        return {
            "statusCode": 200,
            "message": f"Successfully processed message {message_id}",
        }

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "error_message": f"Failed to process message {message_id}",
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )

        return {"statusCode": 500, "error": f"Failed to process message {message_id}"}


def get_ssm_parameters(
    env_stage, logger: LambdaStructuredLogger, event, context_data=None
):
    """
    Retrieve required SSM parameters for the given environment stage.

    Args:
        env_stage (str): Environment stage (e.g., 'dev', 'prod')
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        context_data (dict): Additional context for logging

    Returns:
        dict: Dictionary containing the retrieved parameters
    """
    if context_data is None:
        context_data = {}

    # Define parameter paths
    parameter_paths = {
        "bearer": f"/{env_stage}/axon/api/bearer",
        "base_url": f"/{env_stage}/axon/api/base_url",
    }

    parameters = {}
    failed_parameters = []
    start_time = time.time()

    try:
        # Get all parameters in batch (more efficient than individual calls)
        parameter_names = list(parameter_paths.values())

        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True,  # This will decrypt SecureString parameters
        )

        # Calculate response time
        response_time_ms = (time.time() - start_time) * 1000

        # Process successful parameters
        for param in response.get("Parameters", []):
            param_name = param["Name"]
            param_value = param["Value"]

            # Find the logical name for this parameter path
            logical_name = None
            for logical, path in parameter_paths.items():
                if path == param_name:
                    logical_name = logical
                    break

            if logical_name:
                parameters[logical_name] = param_value

        # Process failed parameters
        failed_parameters = response.get("InvalidParameters", [])

        if failed_parameters:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="log_ssm_parameter_collection",
                context_data={
                    **context_data,
                    "env_stage": env_stage,
                    "failed_parameters": failed_parameters,
                    "retrieved_parameters": list(parameters.keys()),
                    "operation": "ssm_parameter_retrieval_partial_failure",
                },
            )

            raise ValueError(f"Failed to retrieve SSM parameters: {failed_parameters}")

        # Log retrieval status
        logger.log_ssm_parameter_collection(
            parameter_names=list(parameters.keys()),
            parameters_collected=parameters,
            response_time_ms=response_time_ms,
            invalid_parameters=failed_parameters if failed_parameters else None,
            **context_data,
        )

        return parameters

    except ClientError as e:
        response_time_ms = (time.time() - start_time) * 1000
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]

        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_ssm_parameter_collection",
            context_data={
                **context_data,
                "env_stage": env_stage,
                "error_code": error_code,
                "error_message": error_message,
                "failed_parameters": parameter_names,
                "operation": "ssm_parameter_retrieval_client_error",
                "response_time_ms": response_time_ms,
            },
        )

        raise Exception(f"SSM Client Error: {error_code} - {error_message}")

    except Exception as e:
        response_time_ms = (time.time() - start_time) * 1000
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_ssm_parameter_collection",
            context_data={
                **context_data,
                "env_stage": env_stage,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "ssm_parameter_retrieval_unexpected_error",
                "response_time_ms": response_time_ms,
            },
        )

        raise


def construct_evidence_file_url(
    base_url: str, source_agency: str, evidence_id: str, evidence_file_id: str
) -> str:
    """
    Construct the full URL for accessing an evidence file.

    Args:
        base_url: axon base url
        source_agency: The source agency GUID
        evidence_id: The evidence ID GUID
        evidence_file_id: The evidence file ID GUID

    Returns:
        str: The constructed URL
    """
    base_url = base_url.rstrip("/")
    url = f"{base_url}/api/v1/agencies/{source_agency}/evidence/{evidence_id}/files/{evidence_file_id}"

    return url


def download_evidence_file(url: str, evidence_file_id: str, bearer_token: str) -> str:
    """
    Download an evidence file to Lambda's /tmp directory.

    Args:
        url: The complete URL to download the file from
        evidence_file_id: The evidence file ID (used for filename)
        bearer_token: bearer token

    Returns:
        str: The full path to the downloaded file

    Raises:
        Exception: If file writing fails
    """
    # Set up headers
    headers = {"Authorization": f"Bearer {bearer_token}", "Accept": "*/*"}

    # Create filename and full path
    filename = f"{evidence_file_id}"
    file_path = f"/tmp/{filename}"

    http = urllib3.PoolManager()

    try:
        # Make the request with streaming to minimize memory usage
        response = http.request(
            "GET", url, headers=headers, timeout=300, preload_content=False
        )

        if response.status != 200:
            raise Exception(f"HTTP {response.status}: {response.reason}")

        # Stream the file directly to ephemeral storage (/tmp)
        with open(file_path, "wb") as f:
            for chunk in response.stream(8192):  # 8KB chunks
                f.write(chunk)

        # Verify file was written
        if not os.path.exists(file_path):
            raise Exception(f"File was not created at {file_path}")

        file_size = os.path.getsize(file_path)
        file_size = round(file_size / (1024 * 1024 * 1024), 2)
        print(f"Successfully downloaded file: {file_path} ({file_size}GB)")

        return file_path

    except Exception as e:
        print(f"Failed to download file from {url}: {str(e)}")
        # Clean up partial file if it exists
        if os.path.exists(file_path):
            os.remove(file_path)
        raise


def calculate_file_checksum(file_path: str) -> str:
    """
    Calculate checksum of a file.

    Args:
        file_path: Path to the file

    Returns:
        str: Hexadecimal checksum string

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If algorithm is not supported
    """
    # Create hash object
    hash_obj = hashlib.new("sha256")

    try:
        # Read file in chunks to handle large files efficiently
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_obj.update(chunk)

        checksum = hash_obj.hexdigest()
        print(f"Calculated sha256 checksum for {file_path}: {checksum}")
        return checksum

    except FileNotFoundError:
        print(f"File not found: {file_path}")
        raise
    except Exception as e:
        print(f"Error calculating checksum for {file_path}: {str(e)}")
        raise
