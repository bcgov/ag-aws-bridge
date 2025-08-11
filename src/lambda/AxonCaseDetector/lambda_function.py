import json
import boto3
import requests
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from datetime import datetime, timezone
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager

def lambda_handler(event, context):
    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Initialize AWS SSM client
    ssm_client = boto3.client('ssm')
    
    # Initialize database manager
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')
    db_manager = get_db_manager(env_param=env_stage)

    # Log the start of the function
    logger.log_start(
        event="simple_lambda_execution",
        job_id=context.aws_request_id
    )
    
    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')
    logger.info(f"Loading configuration for environment: {env_stage}")
            
    
    # Collect SSM parameters
    try:
        # Define the parameter names you want to retrieve
        # 'host': f'/{env_stage}/bridge/tracking-db/host',
        parameter_names = [
            f'/{env_stage}/axon/api/get_cases_url_filter_path',
            f'/{env_stage}/axon/api/authentication_url',
            f'/{env_stage}/axon/api/bearer',
            f'/{env_stage}/axon/api/client_secret',
            f'/{env_stage}/axon/api/client_id',
            f'/{env_stage}/axon/api/case_detector_interval_mins'
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
            logger.log_error("SSM Param Retrieval", details={"invalid_parameters": response['InvalidParameters']})
            raise Exception(f"Failed to retrieve some parameters: {response['InvalidParameters']}")
        
        # Add API method
        parameters["method"] = "POST"
        api_url = parameters['/dev-test/axon/api/authentication_url']

        
        # Get API bearer token
        try:
           payload = {
                "client_id" : parameters["/dev-test/axon/api/client_id"],
                "grant_type" : "client_credentials",
                "client_secret" : parameters["/dev-test/axon/api/client_secret"]
           }
            response = requests.post(api_url, data=payload)
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes

            # Log success and return the response
            logger.log_success(
                event="api_call",
                details={"status_code": response.status_code, "url": api_url}
            )
            data = response.json() 
            parameters["/dev-test/axon/api/bearer"] = data.get("access_token")

        except requests.exceptions.RequestException as e:
            logger.log_error(
                event="api_call_failed",
                details={"error": str(e), "url": api_url}
            )
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error getting bearer token',
                    'error': str(e)
                })
            }
        
        # Prepare headers for the GET request (assuming bearer token authentication)
        headers = {
            'Authorization': f"Bearer {parameters['/dev-test/axon/api/bearer']}",
            'Content-Type': 'application/json'
        }
        # Make the GET request to the API endpoint
        api_url = parameters['/dev-test/axon/api/get_cases_url_filter_path']

        # Get current UTC time
        current_utc_time = datetime.now(timezone.utc)

        # Get interval to use
        case_detector_interval_mins = parameters["/dev-test/axon/api/case_detector_interval_mins"]

        # Substract 5 minutes to get second UTC time
        fivemins_past = current_utc_time - timedelta(minutes=case_detector_interval_mins)
        
        # make filter string
        filter_string = f"createdOn in {fivemins_past} to {current_utc_time}"

        try:
            params = {
                "filter": filter_string
            }
            response = requests.get(api_url, headers=headers,params=params, timeout=10)
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes
            
            # Get response time
            response_time = response.elapsed.total_seconds()
            
            logger.log_api_call(event="call to Axon Get Cases", method="POST", status_code= response.status_code, 
            response_time = response_time,   job_id=context.aws_request_id)

            # Log success and return the response
            logger.log_success(
                event="api_call",
                details={"status_code": response.status_code, "url": api_url}
            )

            if response.status_code == 200:
                # Parse JSON response
                json_data = response.json()
                 # Extract top-level meta information
                meta = json_data.get("meta", {})
                offset = meta.get("offset")
                limit = meta.get("limit")
                count = meta.get("count")

                if count >= 1:
                    data = json_data.get("data", [])
                     # Get response time
                    response_time = response.elapsed.total_seconds()
            
                    logger.log_api_call(event="call to Axon Get Cases successful. Found at least 1 case", method="POST", status_code= response.status_code, 
                    response_time = response_time,   job_id=context.aws_request_id)

                    for item in data:
                        # Extract fields from each item
                        item_type = item.get("type")
                        item_id = item.get("id")
                        attributes = item.get("attributes", {})

                        # Extract attributes
                        title = attributes.get("title")
                        description = attributes.get("description")
                        created_on = attributes.get("createdOn")
                        status = attributes.get("status")
                        owner = attributes.get("owner", {})
                        owner_id = owner.get("id")
                        owner_agency = owner.get("relationships", {}).get("agency", {}).get("data", {}).get("id")

                        # Extract caseSharedFrom (list)
                        case_shared_from = attributes.get("caseSharedFrom", [])
                result = {"statusCode": 200, "body": "Success calling API, at least 1 result found."}
                return result
            else:
                # Get response time
                response_time = response.elapsed.total_seconds()
            
                logger.log_api_call(event="call to Axon Get Cases, no new cases found", method="POST", status_code= response.status_code, 
                response_time = response_time,   job_id=context.aws_request_id)
                result = {"statusCode": 200, "body": "Success calling API, no results found."}
                return result
            
        except requests.exceptions.RequestException as e:
            logger.log_error(
                event="api_call_failed",
                details={"error": str(e), "url": api_url}
            )
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'Error making GET request',
                    'error': str(e)
                })
            }

    except Exception as e:
        logger.log_error(
            event="SSM Param Retrieval",
            details={"error": str(e)}
        )
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error retrieving parameters',
                'error': str(e)
            })
        }