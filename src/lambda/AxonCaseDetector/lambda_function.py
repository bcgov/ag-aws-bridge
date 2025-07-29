import json
import boto3

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
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
    # collect ssm params
    try:
        # Define the parameter names you want to retrieve
        parameter_names = [
            '/dev-test/axon/api/get_cases_url_filter_path',
            '/dev-test/axon/api/authentication_url',
            '/dev-test/axon/api/bearer',
            '/dev-test/axon/api/client_secret'
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
            print(f"Invalid parameters: {response['InvalidParameters']}")
            logger.log_error("SSM Param Retrieval",)
            raise Exception(f"Failed to retrieve some parameters: {response['InvalidParameters']}")
            
    except Exception as e:
        print(f"Error retrieving parameters: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error retrieving parameters',
                'error': str(e)
            })
        }
    }
