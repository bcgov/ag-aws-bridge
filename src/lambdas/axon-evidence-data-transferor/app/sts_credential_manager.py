import boto3
import json
import logging
import os
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, BotoCoreError

# Configure logging
logger = logging.getLogger(__name__)

class STSCredentialManager:
    def __init__(self, region_name='ca-central-1'):
        self.sts_client = boto3.client('sts', region_name=region_name)
        self.ssm_client = boto3.client('ssm', region_name=region_name)
        self.credentials = None
        self.expiration = None
        self.env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        
        # Validate environment stage
        if self.env_stage not in ['dev-test', 'prod']:
            raise ValueError(f"Invalid ENV_STAGE: {self.env_stage}. Must be 'dev-test' or 'prod'")
        
        logger.info(f"Initialized STSCredentialManager for environment: {self.env_stage}")
    
    def get_iam_parameters(self):
        """
        Retrieve role_arn and external_id from SSM Parameter Store
        
        Returns:
            dict: Contains 'role_arn' and 'external_id' or None if failed
        """
        base_path = f"/{self.env_stage}/edt/s3/iam"
        
        parameters_to_get = {
            'role_arn': f"{base_path}/role_arn",
            'external_id': f"{base_path}/external_id"
        }
        
        try:
            logger.info(f"Retrieving IAM parameters from SSM")
            logger.info(f"Base path: {base_path}")
            
            # Get parameters in batch
            response = self.ssm_client.get_parameters(
                Names=list(parameters_to_get.values()),
                WithDecryption=True  # Decrypt SecureString parameters
            )
            
            # Check if all parameters were found
            found_names = [param['Name'] for param in response['Parameters']]
            missing_params = []
            
            for key, param_name in parameters_to_get.items():
                if param_name not in found_names:
                    missing_params.append(param_name)
            
            if missing_params:
                logger.error(f"Missing SSM parameters: {missing_params}")
                return None
            
            # Build result dictionary
            result = {}
            for param in response['Parameters']:
                for key, param_name in parameters_to_get.items():
                    if param['Name'] == param_name:
                        result[key] = param['Value']
                        break
            
            logger.info("Successfully retrieved IAM parameters from SSM")
            logger.info(f"Role ARN: {result['role_arn']}")
            # Don't log external_id for security
            
            return result
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            logger.error(f"SSM ClientError: {error_code} - {error_message}")
            
            if error_code == 'ParameterNotFound':
                logger.error(f"Parameters not found in path: {base_path}")
            elif error_code == 'AccessDenied':
                logger.error("Lambda execution role lacks SSM permissions")
            
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error retrieving SSM parameters: {str(e)}")
            return None
    
    def assume_role_with_job_id(self, job_id, duration=3600):
        """
        Assume IAM role using job_id as session name and SSM parameters
        
        Args:
            job_id (str): Job identifier used as RoleSessionName
            duration (int): Duration in seconds (900-43200, default 3600)
        
        Returns:
            dict: Temporary credentials or None if failed
        """
        
        # Get IAM parameters from SSM
        iam_params = self.get_iam_parameters()
        if not iam_params:
            logger.error("Failed to retrieve IAM parameters from SSM")
            return None
        
        # Use received BRIDGE SQS message contents "job_id" directly as session name
        session_name = job_id
        
        # Prepare request parameters
        assume_role_params = {
            'RoleArn': iam_params['role_arn'],
            'RoleSessionName': session_name,
            'ExternalId': iam_params['external_id'],
            'DurationSeconds': duration
        }
        
        try:
            logger.info(f"Attempting to assume role for job: {job_id}")
            logger.info(f"Role ARN: {iam_params['role_arn']}")
            logger.info(f"Session name: {session_name}")
            logger.info(f"Duration: {duration} seconds")
            logger.info(f"Environment: {self.env_stage}")
            
            response = self.sts_client.assume_role(**assume_role_params)
            
            # Extract credentials
            credentials = response['Credentials']
            assumed_role_user = response['AssumedRoleUser']
            
            logger.info(f"Successfully assumed role for job: {job_id}")
            logger.info(f"Assumed Role ARN: {assumed_role_user['Arn']}")
            logger.info(f"Credentials expire at: {credentials['Expiration']}")
            
            # Store for internal use
            self.credentials = credentials
            self.expiration = credentials['Expiration']
            
            return {
                'AccessKeyId': credentials['AccessKeyId'],
                'SecretAccessKey': credentials['SecretAccessKey'],
                'SessionToken': credentials['SessionToken'],
                'Expiration': credentials['Expiration'],
                'AssumedRoleArn': assumed_role_user['Arn']
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            
            logger.error(f"AWS ClientError: {error_code} - {error_message}")
            
            if error_code == 'AccessDenied':
                logger.error("Check if your IAM user/role has sts:AssumeRole permission")
                logger.error("Also verify the target role's trust policy allows your account")
            elif error_code == 'InvalidUserID.NotFound':
                logger.error("The specified role ARN does not exist")
            elif error_code == 'ValidationError':
                logger.error("Check the External ID or other parameters")
            
            return None
            
        except BotoCoreError as e:
            logger.error(f"BotoCore error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            return None
    
    def is_expired(self, buffer_minutes=5):
        """Check if credentials are expired or will expire soon"""
        if not self.expiration:
            return True
        
        buffer_time = datetime.now(self.expiration.tzinfo) + timedelta(minutes=buffer_minutes)
        return buffer_time >= self.expiration
    
    def get_caller_identity(self):
        """Get information about the current credentials"""
        try:
            response = self.sts_client.get_caller_identity()
            logger.info(f"Current identity: {response}")
            return response
        except Exception as e:
            logger.error(f"Failed to get caller identity: {e}")
            return None


def lambda_handler(event, context):
    """
    Lambda function that processes jobId from SQS and assumes role using SSM parameters
    """
    # Initialize the credential manager
    cred_manager = STSCredentialManager()
    
    try:
        # Parse SQS message - extract jobId
        message = json.loads(event['Records'][0]['body'])
        job_id = message['jobId']
        
        logger.info(f"Processing job: {job_id}")
        
        # Check current identity (for logging)
        current_identity = cred_manager.get_caller_identity()
        logger.info(f"Lambda executing as: {current_identity}")
        
        # Assume role using jobId as session name
        temp_creds = cred_manager.assume_role_with_job_id(
            job_id=job_id,
            duration=1800  # 30 minutes
        )
        
        if not temp_creds:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Failed to assume role for job: {job_id}')
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Job completed successfully',
                'jobId': job_id,
                'environment': cred_manager.env_stage,
                'credentials_expire': temp_creds['Expiration'].isoformat(),
                'assumed_role': temp_creds['AssumedRoleArn']
            })
        }
        
    except KeyError as e:
        logger.error(f"Missing required field in SQS message: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid SQS message format: missing {str(e)}')
        }
    except Exception as e:
        logger.error(f"Job processing failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Job processing failed: {str(e)}')
        }

      
# Standalone usage example
if __name__ == "__main__":
    # Set environment variable (normally set in Lambda configuration)
    os.environ['ENV_STAGE'] = 'dev-test'
    
    # Initialize credential manager
    cred_manager = STSCredentialManager()
    
    # Test with a sample job ID
    test_job_id = "test-job-12345"
    temp_creds = cred_manager.assume_role_with_job_id(
        job_id=test_job_id,
        duration=1800
    )
    
    if temp_creds:
        print(f"Successfully obtained credentials for job: {test_job_id}")
        print(f"Environment: {cred_manager.env_stage}")
        print(f"Credentials expire: {temp_creds['Expiration']}")
    else:
        print("Failed to obtain credentials")