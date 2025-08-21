import json
import os
import boto3
import requests
from urllib.parse import urljoin
from datetime import datetime, timezone
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
        event: Lambda event object
        context_data (dict): Additional context for logging

    Returns:
        dict: Dictionary containing the retrieved parameters
    """
    if context_data is None:
        context_data = {}

    # Define parameter paths
    parameter_paths = {
        "base_url": f"/{env_stage}/axon/api/base_url",
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


def store_access_token(env_stage, access_token, token_data, logger, event, context_data=None):
    """
    Store the access token in SSM Parameter Store as a SecureString.

    Args:
        env_stage (str): Environment stage (e.g., 'dev', 'prod')
        access_token (str): The access token to store
        token_data (dict): Full token response data for metadata
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        context_data (dict): Additional context for logging

    Returns:
        bool: True if successful, raises exception if failed
    """
    if context_data is None:
        context_data = {}

    # Define the parameter path for the bearer token
    bearer_parameter_name = f"/{env_stage}/axon/api/bearer"
    
    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="log_token_storage",
        context_data={
            **context_data,
            "parameter_name": bearer_parameter_name,
            "operation": "token_storage_start",
        },
    )

    try:
        # Prepare parameter description with token metadata
        description = f"Bearer token for Axon API - {env_stage} environment"
        # if token_data.get("expires_in"):
        #     description += f" (expires in {token_data.get('expires_in')} seconds)"

        if token_data.get("expires_on"):
            expires_on_ms = int(token_data["expires_on"])
            timestamp_seconds = expires_on_ms / 1000
            expiry_dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)
            expiry_readable = expiry_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
            description += f" (expires on {expiry_readable})"
            print(description)

        # Store the token as a SecureString
        response = ssm_client.put_parameter(
            Name=bearer_parameter_name,
            Value=access_token,
            Type="SecureString",
            Description=description,
            Overwrite=True,  # Allow overwriting existing parameter
        )

        # Log successful storage
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="log_token_storage",
            context_data={
                **context_data,
                "parameter_name": bearer_parameter_name,
                "parameter_version": response.get("Version"),
                "parameter_tier": response.get("Tier"),
                "token_length": len(access_token),
                "operation": "token_storage_success",
            },
        )

        return True

    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]

        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_storage",
            context_data={
                **context_data,
                "parameter_name": bearer_parameter_name,
                "error_code": error_code,
                "error_message": error_message,
                "operation": "token_storage_client_error",
            },
        )

        raise Exception(f"SSM Client Error during token storage: {error_code} - {error_message}")

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_storage",
            context_data={
                **context_data,
                "parameter_name": bearer_parameter_name,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "token_storage_unexpected_error",
            },
        )

        raise


def get_access_token(ssm_parameters, logger, event, context_data=None):
    """
    Retrieve access token from the third-party API using client credentials.

    Args:
        ssm_parameters (dict): Dictionary containing authentication parameters
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        context_data (dict): Additional context for logging

    Returns:
        tuple: (access_token, token_data) - The access token and full response data

    Raises:
        Exception: If token retrieval fails
    """
    if context_data is None:
        context_data = {}

    # Extract required parameters
    base_url = ssm_parameters.get("base_url")
    auth_url = ssm_parameters.get("authentication_url")
    client_id = ssm_parameters.get("client_id")
    client_secret = ssm_parameters.get("client_secret")
    grant_type = ssm_parameters.get("grant_type", "client_credentials")
    full_auth_url = urljoin(base_url, auth_url)

    # Validate required parameters
    required_params = ["base_url", "authentication_url", "client_id", "client_secret"]
    missing_params = [param for param in required_params if not ssm_parameters.get(param)]
    
    if missing_params:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_authentication",
            context_data={
                **context_data,
                "missing_parameters": missing_params,
                "operation": "token_retrieval_validation_error",
            },
        )
        raise ValueError(f"Missing required authentication parameters: {missing_params}")

    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="log_token_authentication",
        context_data={
            **context_data,
            "full_auth_url": full_auth_url,
            "client_id": client_id,
            "grant_type": grant_type,
            "operation": "token_retrieval_start",
        },
    )

    try:
        # Prepare the authentication request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }

        # Prepare the request body
        data = {
            "grant_type": grant_type,
            "client_id": client_id,
            "client_secret": client_secret
        }

        # Make the token request
        response = requests.post(
            full_auth_url,
            headers=headers,
            data=data,
            timeout=30  # 30 second timeout
        )

        # Log the response status
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="log_token_authentication",
            context_data={
                **context_data,
                "status_code": response.status_code,
                "response_time_ms": response.elapsed.total_seconds() * 1000,
                "operation": "token_retrieval_response_received",
            },
        )

        # Check if the request was successful
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            
            if not access_token:
                logger.log(
                    event=event,
                    level=LogLevel.ERROR,
                    status=LogStatus.FAILURE,
                    message="log_token_authentication",
                    context_data={
                        **context_data,
                        "response_keys": list(token_data.keys()),
                        "operation": "token_retrieval_missing_token",
                    },
                )
                raise ValueError("Access token not found in response")

            # Log successful token retrieval (without exposing the token)
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="log_token_authentication",
                context_data={
                    **context_data,
                    "token_type": token_data.get("token_type", "unknown"),
                    "expires_in": token_data.get("expires_in"),
                    "has_refresh_token": "refresh_token" in token_data,
                    "operation": "token_retrieval_success",
                },
            )

            return access_token, token_data

        else:
            # Handle HTTP errors
            error_detail = "Unknown error"
            try:
                error_response = response.json()
                error_detail = error_response.get("error_description", 
                              error_response.get("error", "Unknown error"))
            except:
                error_detail = response.text[:200]  # First 200 chars of response

            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="log_token_authentication",
                context_data={
                    **context_data,
                    "status_code": response.status_code,
                    "error_detail": error_detail,
                    "operation": "token_retrieval_http_error",
                },
            )

            raise Exception(f"Token request failed with status {response.status_code}: {error_detail}")

    except requests.exceptions.Timeout:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_authentication",
            context_data={
                **context_data,
                "error": "Request timeout",
                "operation": "token_retrieval_timeout",
            },
        )
        raise Exception("Token request timed out")

    except requests.exceptions.ConnectionError:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_authentication",
            context_data={
                **context_data,
                "error": "Connection error",
                "operation": "token_retrieval_connection_error",
            },
        )
        raise Exception("Failed to connect to authentication server")

    except requests.exceptions.RequestException as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_authentication",
            context_data={
                **context_data,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "token_retrieval_request_error",
            },
        )
        raise Exception(f"Token request failed: {str(e)}")

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_token_authentication",
            context_data={
                **context_data,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "token_retrieval_unexpected_error",
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
        logger.log_start(event="simple_lambda_execution", job_id=request_id)

        # Retrieve SSM parameters
        ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

        # Get access token from third-party API
        access_token, token_data = get_access_token(ssm_parameters, logger, event, base_context)

        # Store the access token in SSM Parameter Store
        store_access_token(env_stage, access_token, token_data, logger, event, base_context)

        # Log successful completion
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="lambda_execution_complete",
            context_data={
                **base_context,
                "operation": "lambda_execution_success",
            },
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully retrieved and stored access token",
                "token_length": len(access_token) if access_token else 0,
                "token_stored_at": f"/{env_stage}/axon/api/bearer",
            })
        }

    except Exception as e:
        # Log the error
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="lambda_execution_error",
            context_data={
                **base_context,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "lambda_execution_failure",
            },
        )

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error",
                "error": str(e)
            })
        }