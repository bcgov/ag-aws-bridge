import json
import os
import time
from typing import Dict, Any, List
import boto3
from botocore.exceptions import ClientError
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

        records = event.get('Records', [])
        record = records[0]
        message_id = record.get('messageId')
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "message": f'Processing message {message_id}',
            },
        )
        
        # Parse the message body
        message_body = json.loads(record.get('body', '{}'))
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "message": f'Message content: {message_body}',
            },
        )

        db_manager = get_db_manager(env_param_in=env_stage)
        sample_files = db_manager.get_sample_evidence_files(5)
        logger.info("Sample evidence files:")
        for file_record in sample_files:
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_downloader",
                context_data={
                    "env_stage": env_stage,
                    "file_record": f"  ID: {file_record['evidence_id']}, Job: {file_record['job_id']}, FileID: {file_record['evidence_file_id']}",
                },
            )
        
        # Retrieve SSM parameters
        ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

        # evidence_id = message_body.get('evidence_id')
        # db_manager = get_db_manager(env_param_in=env_stage)
        # evidence_file = db_manager.get_evidence_file(evidence_id)
        # logger.log(
        #     event=event,
        #     level=LogLevel.INFO,
        #     status=LogStatus.SUCCESS,
        #     message="axon_evidence_downloader_file",
        #     context_data={
        #         "env_stage": env_stage,
        #         "file": f'Count: {list(evidence_file.keys()).count}',
        #     },
        # )

        # # Get access token from third-party API
        # access_token, token_data = get_access_token(ssm_parameters, logger, event, base_context)

        # # Store the access token in SSM Parameter Store
        # store_access_token(env_stage, access_token, token_data, logger, event, base_context)

        # # Log successful completion
        # logger.log(
        #     event=event,
        #     level=LogLevel.INFO,
        #     status=LogStatus.SUCCESS,
        #     message="lambda_execution_complete",
        #     context_data={
        #         **base_context,
        #         "operation": "lambda_execution_success",
        #     },
        # )

        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "message": f'Successfully processed message {message_id}',
            },
        )

        return {
                'statusCode': 200,
                'message': f'Successfully processed message {message_id}'
            }

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_downloader",
            context_data={
                "env_stage": env_stage,
                "error_message": f'Failed to process message {message_id}',
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )

        return {
            'statusCode': 500,
            'error': f'Failed to process message {message_id}'
        }
    

def get_ssm_parameters(env_stage, logger: LambdaStructuredLogger, event, context_data=None):
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
            **context_data
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
