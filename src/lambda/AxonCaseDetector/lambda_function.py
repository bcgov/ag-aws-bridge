import json
import boto3
import requests
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from datetime import datetime, timezone

def lambda_handler(event, context):
    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Initialize AWS SSM client
    ssm_client = boto3.client('ssm')
    
    # Log the start of the function
    logger.log_start(
        event="simple_lambda_execution",
        job_id=context.aws_request_id
    )
    
    # Collect SSM parameters
    try:
        # Define the parameter names you want to retrieve
        parameter_names = [
            '/dev-test/axon/api/get_cases_url_filter_path',
            '/dev-test/axon/api/authentication_url',
            '/dev-test/axon/api/bearer',
            '/dev-test/axon/api/client_secret',
            '/dev-test/axon/api/client_id'
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

        # Substract 5 minutes to get second UTC time
        fivemins_past = current_utc_time - timedelta(minutes=5)
        
        # make filter string
        filter_string = f"createdOn in {fivemins_past} to {current_utc_time}"

        try:
            params = {
                "filter": filter_string
            }
            response = requests.get(api_url, headers=headers,params=params, timeout=10)
            response.raise_for_status()  # Raise an exception for 4xx/5xx status codes

            # Log success and return the response
            logger.log_success(
                event="api_call",
                details={"status_code": response.status_code, "url": api_url}
            )
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'GET request successful',
                    'data': response.json()  # Assuming the response is JSON
                })
            }

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