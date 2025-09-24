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
    PROCESS_NAME = "axon-evidence-metadata-updater"
    SIZE_THRESHOLD_BYTES = 10 * 1024 * 1024 * 1024  # 10GB
    SQS_BATCH_SIZE = 10

logger = LambdaStructuredLogger()
db_manager = None

# Global configuration cache
_lambda_config = None

def get_lambda_config(ssm=None,context_data=None) -> Dict[str, str]:
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
            'axon_agency_id': f'/{env_stage}/axon/api/agency_id',
            'axon_base_url': f'/{env_stage}/axon/api/base_url',
            'axon_client_id': f'/{env_stage}/axon/api/client_id',
            'axon_client_secret': f'/{env_stage}/axon/api/client_secret',
            'axon_auth_url': f'/{env_stage}/axon/api/authentication_url',
            'axon_get_case_details_url': f'/{env_stage}/axon/api/get_case_details_url',
            'axon_metadata_field_id_transfer_utc' : f'/{env_stage}/axon/api/metadata_field_id_transfer_utc',
            'axon_metadata_field_id_transfer_state' : f'/{env_stage}/axon/api/metadata_field_id_transfer_state',
            'metadata_field_id_transfer_state_downloaded_id' : f'{env_stage}/axon/api/metadata_field_id_transfer_state_downloaded_id',
            'axon_get_evidence_details_url': f'/{env_stage}/axon/api/get_evidence_details_url',
            'q-transfer-prepare.fifo': f'/{env_stage}/bridge/sqs-queues/url_q-transfer-prepare.fifo'
           
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
    if db_manager is None:
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        db_manager = get_db_manager(env_param_in=env_stage)
        db_manager._initialize_pool()
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Initialized DatabaseManager for environment: {env_stage}")

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
    return {
        'Id': job_id,  # Must be unique within the batch
        'MessageBody': json.dumps({
            'job_id': job_id
           
        }),
        'MessageGroupId': f"job-{job_id}",  # Required for FIFO queues
        'MessageDeduplicationId': f"{job_id}-{current_utc_time}",  # Required for FIFO queues
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
                logger.log_sqs_message_sent(queue_url=queue_url,mssage_id = response.get['MessageId'], message_body=batch, response_time_ms=1)
                
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
    if job_status_code:
        status_identifier = str(job_status_code["identifier"])
        db_manager.update_job_status(
            job_id=job_id,
            status_code=status_identifier,
            job_msg=msg,
            last_modified_process=process_name
        )

def update_evidence_status( evidence_ids:str)->bool:
    job_status_code = db_manager.get_status_code_by_value(value="METADATA-UPDATED")
    try:
        if (job_status_code and evidence_ids):
            for item in evidence_ids.split(','):
                update_evidence_file_state(item,job_status_code,"lambda:" + Constants.PROCESS_NAME)
        logger.log_success( event=Constants.PROCESS_NAME, message=f"Successfully updated {len(evidence_ids)} evidence files")
        return True
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        return False






def updateAxonTransferState(evidence_ids: str, keyId: str, config: Dict[str, str]) -> bool:
    """
    Updates the transfer state of evidence in the Axon system.

    Args:
        evidence_id (str): The ID of the evidence to update.
        keyId (str): A key identifier (e.g., for tracking or payload).
        config (Dict[str, str]): Configuration dictionary containing API credentials and URLs.

    Returns:
        bool: True if the update was successful, False otherwise.

    Raises:
        Exception: If the API call fails or the response is invalid.
    """
    try:
        headers = {
            'Authorization': f'Bearer {config["axon_bearer_token"]}',
            'Content-Type': 'application/json'
        }
        
        # Build the API URL
        api_url = f"{config['axon_base_url']}api/v1/agencies/{config['axon_agency_id']}/evidence/managed-metadata"
        
       
        # Example payload (adjust based on actual API requirements)
        payload = {
            "data" :{
                "attributes" : {
                    "keyId" : keyId,
                    "values" :[
                        config['metadata_field_id_transfer_state_downloaded_id']
                    ],
                    "evidenceIds" : [evidence_ids]

                }
            }
            
        }

        # Make the API call (assuming PUT for updating state)
        response = requests.put(api_url, json=payload, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx, 5xx)

        # Check if the response indicates success
        if response.status_code == 200:
            logger.log_success(f"Successfully updated transfer state for evidence {evidence_ids}")
            return True
        else:
            logger.warning(f"Unexpected response for evidence {evidence_ids}: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Axon Evidence Details API error: {str(e)}",
            url=api_url
        )
        return False
    
    except ValueError as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Invalid response for evidence {evidence_ids}: {str(e)}",
            url=api_url
        )
        return False
    
    except Exception as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Unexpected error for evidence {evidence_ids}: {str(e)}",
            url=api_url
        )
        return False

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


def get_evidence_files( job_id :str, config: Dict[str, str]) -> Dict:
    try:
        results = get_evidence_files_by_job(job_id)
        filtered_results = {k: v.evidence_file_id for k, v in results.items() if v.evidence_transfer_state_code == "DOWNLOADED"}
       
        return filtered_results
    except Exception as db_error:
         # Log failure to send to exception queue, but don't raise to avoid masking original error
    
        logger.log_error( event=Constants.PROCESS_NAME, error=db_error)

def updateAxonTrackingDate(evidence_ids: str, keyId: str, config: Dict[str, str]) -> bool:
    """
    Updates the tracking date of evidence in the Axon system.

    Args:
        evidence_id (str): The ID of the evidence to update.
        keyId (str): A key identifier (e.g., for tracking or payload).
        config (Dict[str, str]): Configuration dictionary containing API credentials and URLs.

    Returns:
        bool: True if the update was successful, False otherwise.

    Raises:
        Exception: If the API call fails or the response is invalid.
    """
    try:
        headers = {
            'Authorization': f'Bearer {config["axon_bearer_token"]}',
            'Content-Type': 'application/json'
        }
        
        # Build the API URL
        api_url = f"{config['axon_base_url']}api/v1/agencies/{config['axon_agency_id']}/evidence/managed-metadata"
        
        current_utc_time = datetime.now(timezone.utc)
       
        # Example payload (adjust based on actual API requirements)
        payload = {
            "data" :{
                "attributes" : {
                    "keyId" : keyId,
                    "values" :[
                        current_utc_time
                    ],
                    "evidenceIds" : [evidence_ids]

                }
            }
            
        }

        # Make the API call (assuming PUT for updating state)
        response = requests.put(api_url, json=payload, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx, 5xx)

        # Check if the response indicates success
        if response.status_code == 200:
            logger.log_success(f"Successfully updated transfer date for evidence {evidence_ids}")
            return True
        else:
            logger.warning(f"Unexpected response for evidence {evidence_ids}: {response.status_code}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Axon Evidence Details API error: {str(e)}",
            url=api_url
        )
        return False
    
    except ValueError as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Invalid response for evidence {evidence_ids}: {str(e)}",
            url=api_url
        )
        return False
    
    except Exception as e:
        logger.error(
            event=Constants.PROCESS_NAME,
            error=f"Unexpected error for evidence {evidence_ids}: {str(e)}",
            url=api_url
        )
        return False
     
# Lambda handler
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        logger.log_start(event="Case Detail and Evidence Filter Start", job_id=context.aws_request_id)
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
            evidence_id  = None
            job_id = None

            if 'job_id' in message_attributes:
                attr = message_attributes['job_id']
                job_id = attr['stringValue'] if attr['dataType'] == 'String' else attr.get('binaryValue')
            if 'evidence_id' in message_attributes:
                attr = message_attributes['source_evidence_idcase_id']
                evidence_id = attr['stringValue'] if attr['dataType'] == 'String' else attr.get('binaryValue')

            if not job_id or not evidence_id:
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception("job_id and evidence_id are required"))
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'job_id and evidence_id are required',
                        'received_event': event
                    })
                }
            config = get_lambda_config()

            evidence_results =  get_evidence_files(job_id,base_context )
            evidence_ids = ','.join(evidence_results)
            logger.log_success(event=Constants.PROCESS_NAME, message=f"Processing evidence for job_id: {job_id}, evidence_id: {evidence_id}, environment: {env_stage}", job_id=job_id)
            proceedToNext = updateAxonTransferState( evidence_ids, config['metadata_field_id_transfer_state_downloaded_id'])
            if proceedToNext:
               logger.log_success(
                event="axon metadata update",
                message="Bearer token retrieval success",
                job_id=context.aws_request_id,
                custom_metadata={"status_code": "200"})
            proceedToNext = update_evidence_status(evidence_ids)
            if proceedToNext:
                logger.log_success(
                    event="axon metadata update",
                    message="Bearer token retrieval success",
                    job_id=context.aws_request_id,
                    custom_metadata={"status_code": "200"}
                    )
            
            proceedToNext = lambda_update_job_status(job_id,"EVIDENCE-METADATA-UPDATED","Evidence Job Status updated", Constants.PROCESS_NAME)
            if proceedToNext:
                sqs = boto3.client('sqs')
                sqs_to_send = create_sqs_message(job_id)

                send_messages_to_queue(sqs,config['q-transfer-prepare.fifo'], sqs_to_send)
                logger.log_success(
                    event="axon metadata update",
                    message="Evidence metadata Job Status updated",
                    job_id=job_id,
                    custom_metadata={"status_code": "200", "msg" : "Evidence metadata Job Status Updated"}
                    )
            else:
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception("Call to update Axon Transfer state resulted in an error"))

        logger.log_success(
            event="Evidence Metadata Updater End",
            message="Successfully completed AxonEvidenceMetadataUpdater execution",
            job_id=context.aws_request_id
        )
        if proceedToNext:
            return {
            'statusCode':  200 ,
            'body': json.dumps({'success': f'Lambda function execution completed'})
            }
        else:
            return { 
            'statusCode':  400 ,
            'body': json.dumps({'error': f'Lambda execution failed: {str(e)}'})
            }
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Lambda execution failed: {str(e)}"))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Lambda execution failed: {str(e)}'})
        }