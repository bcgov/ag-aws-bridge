import json
import boto3
#import requests
import urllib3
import urllib.parse
import os 
import time

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from datetime import datetime, timezone
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager
from datetime import timedelta
from botocore.config import Config

def lambda_handler(event, context):
    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Log the start of the function
    logger.log_start(
        event="Verify Dems Case Start",
        job_id=context.aws_request_id
    )
     # Initialize AWS SSM client
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "standard"})
    ssm_client = boto3.client("ssm", region_name="ca-central-1", config=config)
    # Initialize SQS Client
    sqs = boto3.client('sqs')

      # Initialize database manager
    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

    db_manager = get_db_manager(env_param_in=env_stage)
    db_manager._initialize_pool()

    #initialize HTTP Connection Pool
    http = urllib3.PoolManager()

     # Collect SSM parameters
    try:
        # Define the parameter names you want to retrieve
        # 'host': f'/{env_stage}/bridge/tracking-db/host',
        parameter_names = [
            f'/{env_stage}/isl_endpoint_url ',
            f'/{env_stage}/isl_endpoint_secret ',
            f'/{env_stage}/bridge/sqs-queues/arn_arn_q-transfer-exception',
            f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-detail',
            f'/{env_stage}/bridge/tracking-db/connection-string',
            f'/{env_stage}/axon/api/client_id',
            f'/{env_stage}/axon/api/agency_id',
            f'/{env_stage}/axon/api/case_detector_interval_mins',
            f'/{env_stage}/bridge/sqs-queues/arn_q-axon-case-found'
        ]
        
        # Retrieve multiple parameters at once
        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True  # Set to True for SecureString parameters
        )
        
        # Process the parameters into a dictionary
        parameters = {}
        for param in response['Parameters']:
            parameters[param['Name']] = param['Value']

        if response.get('InvalidParameters'):
            logger.log_error(event = "SSM Param Retrieval", error=none,job_id=context.aws_request_id)
            raise Exception(f"Failed to retrieve some parameters: {response['InvalidParameters']}")
    except Exception as e:
            print(f"Error setting queue attributes: {e}")
            l  logger.log_error(
                event="Error retrieving SSM params",
                error=e,
               job_id = context.aws_request_id
            )

