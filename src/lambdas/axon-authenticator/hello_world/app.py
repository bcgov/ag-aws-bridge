import json
import os
import boto3
from botocore.exceptions import ClientError
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus

# Initialize boto3 client outside handler for reuse
ssm_client = boto3.client("ssm")


def get_ssm_parameters(env_stage, logger, event, context_data=None):
    """
    Retrieve required SSM parameters for the given environment stage.

    Args:
        env_stage (str): Environment stage (e.g., 'dev', 'prod')
        logger: LambdaStructuredLogger instance
        context_data (dict): Additional context for logging

    Returns:
        dict: Dictionary containing the retrieved parameters
    """
    if context_data is None:
        context_data = {}

    # Define parameter paths
    parameter_paths = {
        "authentication_url": f"/{env_stage}/axon/api/authentication_url",
        "client_id": f"/{env_stage}/axon/api/client_id",
        "grant_type": f"/{env_stage}/axon/api/grant_type",
        "client_secret": f"/{env_stage}/axon/api/client_secret",
    }

    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="log_ssm_parameter_collection",
        context_data={
            **context_data,
            "env_stage": env_stage,
            "parameter_paths": list(parameter_paths.values()),
            "operation": "ssm_parameter_retrieval_start",
        },
    )

    parameters = {}
    failed_parameters = []

    try:
        # Get all parameters in batch (more efficient than individual calls)
        parameter_names = list(parameter_paths.values())

        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True,  # This will decrypt SecureString parameters
        )

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

        # Log successful retrieval
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="log_ssm_parameter_collection",
            context_data={
                **context_data,
                "env_stage": env_stage,
                "retrieved_parameters": list(parameters.keys()),
                "parameter_count": len(parameters),
                "operation": "ssm_parameter_retrieval_success",
            },
        )

        return parameters

    except ClientError as e:
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
            },
        )

        raise Exception(f"SSM Client Error: {error_code} - {error_message}")

    except Exception as e:
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
            },
        )

        raise


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    # try:
    #     ip = requests.get("http://checkip.amazonaws.com/")
    # except requests.RequestException as e:
    #     # Send some context about this error to Lambda Logs
    #     print(e)

    #     raise e

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

    # Log the start of the function
    logger.log_start(event="simple_lambda_execution", job_id=request_id)

    # Retrieve SSM parameters
    ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
                # "location": ip.text.replace("\n", "")
            }
        ),
    }
