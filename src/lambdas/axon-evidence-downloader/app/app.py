import json
import os
from typing import Dict, Any, List
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus

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
        
        # # Retrieve SSM parameters
        # ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

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
            },
        )

        return {
            'statusCode': 500,
            'error': f'Failed to process message {message_id}'
        }