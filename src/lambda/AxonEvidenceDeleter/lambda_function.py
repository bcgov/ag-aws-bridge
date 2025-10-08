from enum import Enum
import json
import boto3
import time
from datetime import datetime, timezone
import os
import requests
from typing import List, Dict, Any, Optional, Tuple
import botocore.exceptions
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus

from bridge_tracking_db_layer import (
    DatabaseManager,
    get_evidence_file, create_evidence_file, update_job_counts,
    get_status_code_by_value,get_db_manager,update_evidence_file_state,
    get_evidence_transfer_job, update_job_status, StatusCodes,get_evidence_files_by_job
    
)
    
class Constants:
    PROCESS_NAME = "axon-evidence-deleter"
    SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024 * 1024  # 10GB
    SQS_BATCH_SIZE = 10
    DOWNLOADED_STATE_CODE = 45
    METADATA_UPDATED_JOB_STATUS = "METADATA-UPDATED"
    NORMAL_QUEUE = "normal"
    HTTP_METHOD_GET = "GET"
    HTTP_METHOD_POST = "POST"

logger = LambdaStructuredLogger()
db_manager = None

# Global configuration cache
_lambda_config = None

def get_lambda_config(ssm=None,context_data: Dict[str, Any]=None) -> Dict[str, str]:
    """Get Lambda-specific configuration from SSM Parameter Store."""
    global _lambda_config
    
    if _lambda_config is not None:
        return _lambda_config
    
    start_time = time.time()
    
    try:
        if ssm is None:
            ssm = boto3.client('ssm')
        
        # Get environment stage from environment variable
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Loading configuration for environment: {env_stage}")
        
        # Define SSM parameter paths based on environment
        parameter_paths = {
            'axon_bearer_token': f'/{env_stage}/axon/api/bearer',
            'edt_authenticate_url': f'/{env_stage}/edt/api/authenticate_url',
            'edt_client_id': f'/{env_stage}/edt/api/client_id',
            'edt_grant_type': f'/{env_stage}/edt/api/grant_type',
            'edt_client_secret': f'/{env_stage}/edt/api/client_secret',
            'edt_import_status_url': f'/{env_stage}/edt/api/import_status_poll_url',
            'bridge_tracking_db_connection': f'/{env_stage}/bridge/tracking-db/connection-string',
            'sqs-q-transfer-exception': f'/{env_stage}/bridge/sqs-queues/arn_q-transfer-exception',
            'sqs-q-dems-import': f'/{env_stage}/bridge/sqs-queues/arn_q-dems-import',
        }
        
        parameter_names = list(parameter_paths.values())
        max_parameters_per_request = 10  # AWS SSM limit
        
        # Split parameter names into chunks of 10 or fewer
        parameter_chunks = [parameter_names[i:i + max_parameters_per_request] 
                           for i in range(0, len(parameter_names), max_parameters_per_request)]
        
        config = {}
        all_parameters = []
        missing_params = []
        
        # Fetch parameters in chunks
        for chunk in parameter_chunks:
            response = ssm.get_parameters(
                Names=chunk,
                WithDecryption=True
            )
            all_parameters.extend(response['Parameters'])
        # Calculate response time
        response_time_ms = (time.time() - start_time) * 1000
        # Check if any parameters were not found
        if len(all_parameters) != len(parameter_names):
            missing_params = set(parameter_names) - {p['Name'] for p in all_parameters}
            raise ValueError(f"Missing SSM parameters: {missing_params}")
        
        # Build configuration dictionary
        for param in all_parameters:
            for key, path in parameter_paths.items():
                if param['Name'] == path:
                    config[key] = param['Value']
                    break

        # Log retrieval status
        logger.log_ssm_parameter_collection(
            parameter_names=list(config.keys()),
            parameters_collected=config,
            response_time_ms=response_time_ms,
            invalid_parameters=missing_params if missing_params else None,
            **context_data
        )

        _lambda_config = config
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Lambda configuration loaded from SSM for environment: {env_stage}")
        
        return _lambda_config
        
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise

def initialize_db_manager():
    """Initialize the global DatabaseManager instance."""
    global db_manager
    #print ("in init db manager")
    if db_manager is None:
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
      
        db_manager = get_db_manager(env_param_in=env_stage)
       
        db_manager._initialize_pool()
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Initialized DatabaseManager for environment: {env_stage}")
    else:
        print ("db manager already init'd")

def send_exception_message(exception_queue_url: str, job_id: str, evidence_id: str = None, 
                          source_case_id: str = None, error_message: str = None):
    """Send exception message to exception queue for manual review."""
    try:
        sqs = boto3.client('sqs')
        
        exception_data = {
            'job_id': job_id,
            'evidence_id': evidence_id,
            'source_case_id': source_case_id,
            'error_message': error_message,
            'lambda_function': 'evidence-processor',
            'timestamp': time.time()

  # Will be overridden by SQS
        }
        
        sqs.send_message(
            QueueUrl=exception_queue_url,
            MessageBody=json.dumps(exception_data),
            MessageGroupId=f"exceptions-{job_id}" if job_id else "exceptions-general",
            MessageDeduplicationId=f"{job_id}-{evidence_id}-{hash(error_message)}" if evidence_id else f"{job_id}-general-{hash(error_message)}"
        )
        
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent exception message to queue for job {job_id}, evidence {evidence_id}")
        
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)

def create_sqs_message(job_id: str) -> Dict:
    """Create a properly formatted SQS message for evidence download."""
    current_utc_time = datetime.now(timezone.utc) 
    messageDupId = job_id + " " + str(current_utc_time)
    cleaned_string = ''.join(char for char in messageDupId if char.isalnum())
    
    return {
        'Id': job_id,  # Must be unique within the batch
        'MessageBody': json.dumps({
            'job_id': job_id
           
        }),
        'MessageGroupId': f"job-{job_id}",  # Required for FIFO queues
        'MessageDeduplicationId': f"{cleaned_string}",  # Required for FIFO queues
        'MessageAttributes': {
            'JobId': {
                'StringValue': job_id,
                'DataType': 'String'
            }
        
        }
    }

def send_messages_to_queue(sqs_client, queue_url: str, messages: List[Dict], queue_type: str) -> Dict:
    """Send messages to a specific queue in batches of 10."""
    sent_count = 0
    failed_count = 0
    errors = []
    
    # Process in batches of 10 (SQS limit)
    for i in range(0, len(messages), Constants.SQS_BATCH_SIZE):
        batch = messages[i:i + Constants.SQS_BATCH_SIZE]
        
        try:
            sqs_client.set_queue_attributes( QueueUrl=queue_url,
            Attributes={'KmsMasterKeyId': 'alias/aws/sqs'})

            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=batch
            )
            
            # Count successful messages
            successful = response.get('Successful', [])
            sent_count += len(successful)
            
            # Handle failed messages
            failed = response.get('Failed', [])
            failed_count += len(failed)
            
            for failure in failed:
                error_msg = f"Message {failure['Id']} failed: {failure['Message']} (Code: {failure['Code']})"
                errors.append(error_msg)
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception(error_msg))
                
            
            if successful:
                logger.log_success(event=Constants.PROCESS_NAME, message=f"Successfully sent {len(successful)} messages to {queue_type} queue")
                # if 'MessageId' in response:
                #     sentMsgId = response['MessageId']
                #     logger.log_sqs_message_sent(queue_url=queue_url,message_id =sentMsgId , message_body=batch, response_time_ms=1)
                
        except Exception as e:
            error_msg = f"Failed to send batch to {queue_type} queue: {str(e)}"
            errors.append(error_msg)
            failed_count += len(batch)
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception(error_msg))
    
    return {
        'sent': sent_count,
        'failed': failed_count,
        'errors': errors
    }



def lambda_update_job_status( job_id: str, status_value: str, msg:str, process_name:str):
    """Update job status in the database."""
    job_status_code = db_manager.get_status_code_by_value(value=status_value)
   
    try:
        if job_status_code:
            status_identifier = str(job_status_code["identifier"])
            
            db_manager.update_job_status(
            job_id=job_id,
            status_code=status_identifier,
            job_msg=msg,
            last_modified_process=process_name
            )
            return True
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME + " lambda update job status", error=e)
        return False

# def update_evidence_status( evidence_ids:str)->bool:
#     job_status_code = db_manager.get_status_code_by_value(value=Constants.MET)
#     try:
#         if (job_status_code and evidence_ids):
#             status_identifier = str(job_status_code["identifier"])
#             for item in evidence_ids.split(','):
#                 update_evidence_file_state(item,status_identifier,"lambda:" + Constants.PROCESS_NAME)
#         logger.log_success( event=Constants.PROCESS_NAME, message=f"Successfully updated {len(evidence_ids)} evidence files")
#         return True
#     except Exception as e:
#         logger.log_error(event=Constants.PROCESS_NAME, error=e)
#         return False




def handle_error_and_queue(
    exception: Exception,
    exception_queue_url: str,
    job_id: str,
    evidence_id: Optional[str] = None,
    source_case_id: Optional[str] = None,
    context: str = "general"
) -> None:
    """
    Log an error and send it to the exception queue for manual review.

    Args:
        exception (Exception): The exception to handle.
        exception_queue_url (str): URL of the SQS exception queue.
        job_id (str): Unique identifier for the job.
        evidence_id (Optional[str]): Identifier for the evidence, if applicable.
        source_case_id (Optional[str]): Identifier for the source case, if applicable.
        context (str): Context or component where the error occurred (e.g., "evidence_processing").

    Raises:
        Exception: Re-raises the original exception after logging and queuing.
    """
    try:
        # Log the error in structured JSON format
        log_data = {
            'message': f"Error in {context}",
            'job_id': job_id,
            'evidence_id': evidence_id,
            'source_case_id': source_case_id,
            'error': str(exception),
            'context': context
        }
        logger.log_error(event=Constants.PROCESS_NAME,error=Exception(json.dumps(log_data)))

        # Send the error to the exception queue
        send_exception_message(
            exception_queue_url=exception_queue_url,
            job_id=job_id,
            evidence_id=evidence_id,
            source_case_id=source_case_id,
            error_message=f"Error in {context}: {str(exception)}"
        )

    except Exception as queue_error:
        # Log failure to send to exception queue, but don't raise to avoid masking original error
        logger.log_error( event=Constants.PROCESS_NAME, error=Exception(
            json.dumps({
                'message': f"Failed to send exception message to queue",
                'job_id': job_id,
                'original_error': str(exception),
                'queue_error': str(queue_error),
                'context': context
            }))
        )

    # Re-raise the original exception to maintain error flow
    raise exception

    
def callEdtDemsAPI(job_id : str, dems_import_job_id:str, sqs_message_id:str, context:Any)->bool:
    # call EDT
    # on import error, go to exception , put msg back on sqs queue
    # if import not completed, put sqs back on q-dems-import-status.fifo
    # if import success , do tracking db updates
    
    try:
        edt_auth_url = _lambda_config['edt_authenticate_url']
        payload = {
            "client_id": _lambda_config['edt_client_id'],
            "grant_type": "client_credentials",
            "client_secret": _lambda_config['edt_client_secret']
            }
        start_time = time.perf_counter()
        response = requests.post(edt_auth_url, json=payload, headers={'Content-Type': 'application/x-www-form-urlencoded'})
        # Get response time
        response_time = time.perf_counter() - start_time
        response.raise_for_status()

        logger.log_api_call(event="call to EDT Bearer token", url=edt_auth_url, method=Constants.HTTP_METHOD_POST, status_code= response.status, 
            response_time = response_time,   job_id=context.aws_request_id)
        data = json.loads(response.data.decode('utf-8'))  # Decode response properly
        edt_token = data.get("access_token")
        

    except requests.exceptions.RequestException as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise Exception(f"EDT API error: {str(e)}")
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise
    
    try:
         # call to get import
        edt_get_import_url = _lambda_config['edt_import_status_url']
        headers={'Content-Type': 'application/x-www-form-urlencoded',
                 'Authorization': f"Bearer {edt_token}"}

        start_time = time.perf_counter()
        logger.log(event="EDT api url ", status=LogStatus.IN_PROGRESS, message="API URL constructed : " + edt_get_import_url)

        response = requests.get(url=edt_get_import_url, headers=headers)
         # Get response time
        response_time = time.perf_counter() - start_time
        response.raise_for_status()

        logger.log_api_call(event="call to EDT Get Import", url=edt_auth_url, method=Constants.HTTP_METHOD_GET, status_code= response.status, 
            response_time = response_time,   job_id=context.aws_request_id)
        
         # Parse JSON response
        json_data = json.loads(response.data.decode('utf-8'))

        #need to grab status ?

    except requests.exceptions.RequestException as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise Exception(f"EDT API error: {str(e)}")
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise
    return False

    
# Lambda handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        logger.log_start(event="DEMS Import Status Poller Start", job_id=context.aws_request_id)
        base_context = {
            "request_id": context.aws_request_id,
            "function_name": context.function_name,
            "env_stage": env_stage,
        }
        
        # Init db manager
        initialize_db_manager()
        
        if not event.get("Records"):
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("No records found in event"))
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No records found in event'})
            }

      
        for record in event["Records"]:
            message_attributes = record.get('messageAttributes', {})
            dems_import_job_id = None
            job_id = None

            if 'job_id' in message_attributes:
                attr = message_attributes['job_id']
                job_id = attr['stringValue'] if attr['dataType'] == 'String' else attr.get('binaryValue')
            if 'dems_import_job_id' in message_attributes:
                attr = message_attributes['dems_import_job_id']
                dems_import_job_id = attr['stringValue'] if attr['dataType'] == 'String' else attr.get('binaryValue')

            if not job_id or not dems_import_job_id:
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception("job_id and dems_import_job_id are required"))
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'job_id and dems_import_job_id are required',
                        'received_event': event
                    })
                }
            
            proceedToNext = False

            if callEdtDemsAPI(job_id, dems_import_job_id, base_context):
                 logger.log_success(
                    event="Axon Evidence Deleter ",
                    message="Axon Evidence Deleter completed",
                    job_id=job_id,
                    custom_metadata={"status_code": "200", "msg" : "Axon Evidence Deleter completed."}
                    )
                 return { 'statusCode' : 200, 'body' : 'Axon Evidence Deleter completed.' }
            else:
                logger.log_success(
                    event="Axon Evidence Deleter ",
                    message="Axon Evidence Deleter completed",
                    job_id=job_id,
                    custom_metadata={"status_code": "200", "msg" : "Axon Evidence Deleter completed."}
                    )
            
            

    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Lambda execution failed: {str(e)}"))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Lambda execution failed: {str(e)}'})
        }