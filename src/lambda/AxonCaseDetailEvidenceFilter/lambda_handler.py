from enum import Enum
import json
import boto3
import time
import os
import requests
from typing import List, Dict, Any, Optional, Tuple
import botocore.exceptions
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus

from bridge_tracking_db_layer import (
    DatabaseManager,
    get_evidence_file, create_evidence_file, update_job_counts,
    get_status_code_by_value,get_db_manager,
    get_evidence_transfer_job, update_job_status, StatusCodes,
    
)
    
class Constants:
    PROCESS_NAME = "axon-case-detail-and-evidence-filter"
    SIZE_THRESHOLD_BYTES = 1073741824 # 10 GB in bytes

  # 10GB
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
            'axon_get_evidence_details_url': f'/{env_stage}/axon/api/get_evidence_details_url',
            'normal_download_queue_url': f'/{env_stage}/bridge/sqs-queues/url_q-axon-evidence-download',
            'oversize_download_queue_url': f'/{env_stage}/bridge/sqs-queues/url_q-axon-evidence-download-oversize',
            'transfer_exception_queue_url': f'/{env_stage}/bridge/sqs-queues/url_q-transfer-exception',
            'get_list_case_evidence_url' : f'/{env_stage}/axon/api/get_list_case_evidence_url'
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

def create_sqs_message(job_id: str, evidence_id: str, source_case_id: str, evidence_details: Dict) -> Dict:
    """Create a properly formatted SQS message for evidence download."""
    evidence_file_id = evidence_details.get('fileId')
   
    return {
        'Id': source_case_id,  # Must be unique within the batch
        'MessageBody': json.dumps({
            'job_id': job_id,
            'evidence_id': evidence_id
        }),
        'MessageGroupId': f"job-{job_id}",  # Required for FIFO queues
        'MessageDeduplicationId': f"{job_id}-{evidence_file_id}",  # Required for FIFO queues
        'MessageAttributes': {
            'JobId': {
                'StringValue': job_id,
                'DataType': 'String'
            },
            'evidence_id': {  # Added evidence_id as message attribute
                'StringValue': evidence_id,
                'DataType': 'String'
            }
        }
    }

def create_evidence_files_atomic(files_to_create: List[Dict], job_id: str, process_name: str) -> bool:
    """Create evidence files atomically with proper error handling."""
    try:
        # Prepare transaction queries
        transaction_queries = []
        successful_files = []
        
        # Create evidence files (handle duplicates gracefully)
        for file_data in files_to_create:
            try:
                create_evidence_file(file_data)
                successful_files.append(file_data)
            except Exception as e:
                # Handle unique constraint violation (race condition)
                if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                    print(f"Evidence file {file_data['evidence_id']} already created by another process")
                else:
                    logger.log_error(event=Constants.PROCESS_NAME, error=e)
                    raise  # Re-raise non-duplicate errors
        
        if successful_files:
            # Count files by state
            normal_count = len([f for f in successful_files if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY])
            oversize_count = len([f for f in successful_files if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY_OVERSIZE])
            total_new_files = normal_count + oversize_count
            
            # Update job counts
            current_job = get_evidence_transfer_job(job_id)
            new_to_download = current_job['source_case_evidence_count_to_download'] + total_new_files
            
            update_job_counts(
                job_id=job_id,
                to_download=new_to_download,
                last_modified_process=process_name
            )
            
            logger.log_success( event=Constants.PROCESS_NAME, message=f"Successfully created {len(successful_files)} evidence files ({normal_count} normal, {oversize_count} oversize)")
        
        return True
        
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        return False

def send_batch_sqs_messages(normal_messages: List[Dict], oversize_messages: List[Dict], 
                           normal_queue_url: str, oversize_queue_url: str) -> Dict:
    """Send SQS messages in batches with proper error handling."""
    sqs = boto3.client('sqs')
    results = {
        'normal_queue': {'sent': 0, 'failed': 0, 'errors': []},
        'oversize_queue': {'sent': 0, 'failed': 0, 'errors': []}
    }
    
    # Send normal messages
    if normal_messages:
        normal_result = send_messages_to_queue(sqs, normal_queue_url, normal_messages, "normal")
        results['normal_queue'] = normal_result
    
    # Send oversize messages  
    if oversize_messages:
        oversize_result = send_messages_to_queue(sqs, oversize_queue_url, oversize_messages, "oversize")
        results['oversize_queue'] = oversize_result
    
    total_sent = results['normal_queue']['sent'] + results['oversize_queue']['sent']
    total_failed = results['normal_queue']['failed'] + results['oversize_queue']['failed']
    
    logger.log_success( event=Constants.PROCESS_NAME, message=f"SQS batch results: {total_sent} sent, {total_failed} failed")
    
    return results

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

def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    if size_bytes >= 1024**3:
        return f"{size_bytes / (1024**3):.2f} GB"
    elif size_bytes >= 1024**2:
        return f"{size_bytes / (1024**2):.2f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.2f} KB"
    else:
        return f"{size_bytes} bytes"

def lambda_update_job_status( job_id: str, status_value: str, msg:str):
    """Update job status in the database."""
    job_status_code = db_manager.get_status_code_by_value(value=status_value)
    if job_status_code:
        status_identifier = str(job_status_code["identifier"])
        db_manager.update_job_status(
            job_id=job_id,
            status_code=status_identifier,
            job_msg=msg,
            last_modified_process="llambda: axon case detail and evidence filer"
        )
# API functions using configuration from SSM
def get_case_evidence_from_api(source_case_id: str, job_id: str, config: Dict[str, str], context_data: Dict[str, Any]) -> List[Dict]:
    """Get case evidence from Axon API using configuration from SSM."""
    return_values = []
    try:
        if not config or not config.get("axon_bearer_token"):
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("Configuration or bearer token missing"))
            return return_values

        headers = {
            'Authorization': f'Bearer {config["axon_bearer_token"]}',
            'Content-Type': 'application/json'
        }
        #api_url = f"{config['axon_base_url']}api/v2/agencies/{config['axon_agency_id']}/cases/{source_case_id}/relationships/evidence"
        api_url = f"{config['get_list_case_evidence_url']}"
        api_url = api_url.replace('GUID$$$$',source_case_id)
        
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Calling Axon API: {api_url} for case {source_case_id}")

        start_time = time.perf_counter()
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()

        try:
            json_data = response.json()
          
        except ValueError as e:
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Failed to parse JSON response: {str(e)}"))
            return return_values

        if not json_data:
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("No data returned"))
            return return_values

        api_meta_data = json_data.get('meta', {})
        
        case_evidence_count = int(api_meta_data.get('count'))
        logger.log_success(event=Constants.PROCESS_NAME, message=f"case_evidence_count: {case_evidence_count}")
        evidence_list = json_data.get('data', [])
        
        response_time = (time.perf_counter() - start_time) * 1000
        logger.log_api_call(
            event="Axon Get Case Evidence call",
            url=api_url,
            method="GET",
            status_code=response.status_code,
            response_time=response_time,
            job_id=job_id
        )
        
        if case_evidence_count == 0:
          
            log_data = {
                'case_metadata': {
                    "source_case_id": source_case_id,
                    "source_case_title": "PO-2025-99001",
                    "source_case_evidence_count_total": case_evidence_count,
                    "source_case_evidence_count_to_download": case_evidence_count,
                    "source_case_evidence_count_downloaded": 0
                }
            }
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception(json.dumps(log_data)))
            lambda_update_job_status(
                job_id=job_id,
                status_value="INVALID-CASE",
                msg="No new evidence files found"
                
            )
            return []

        return evidence_list

    except requests.exceptions.RequestException as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise Exception(f"Axon API error: {str(e)}")
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise



def persist_and_queue(
    files_to_create: List[Dict],
    normal_sqs_messages: List[Dict],
    oversize_sqs_messages: List[Dict],
    job_id: str,
    source_case_id: str,
    config: Dict[str, str]
) -> Dict[str, Any]:
    """
    Persist evidence files to the database and queue SQS messages atomically.

    Args:
        files_to_create (List[Dict]): List of evidence file data to store in the database.
        normal_sqs_messages (List[Dict]): SQS messages for normal-sized files.
        oversize_sqs_messages (List[Dict]): SQS messages for oversize files.
        job_id (str): Unique identifier for the job.
        source_case_id (str): Identifier for the source case.
        config (Dict[str, str]): Lambda configuration from SSM.

    Returns:
        Dict[str, Any]: Response with status code and result details.

    Raises:
        ClientError: If AWS services (SQS, database) fail.
        Exception: For unexpected errors during processing.
    """
    PROCESS_NAME = "axon-case-detail-and-evidence-filter"
    exception_queue_url = config['transfer_exception_queue_url']
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

    try:
        if not files_to_create:
            logger.log_success( event=Constants.PROCESS_NAME, message=
                json.dumps({
                    'message': 'No new evidence files to process',
                    'job_id': job_id,
                    'source_case_id': source_case_id,
                    'environment': env_stage
                })
            )
            lambda_update_job_status(
                job_id=job_id,
                status_value="VALID-CASE",
                msg="No new evidence files found"
               
            )
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No new evidence files to process',
                    'job_id': job_id,
                    'source_case_id': source_case_id,
                    'environment': env_stage,
                    'files_processed': 0
                })
            }

        # Step 1: Create evidence files in database transaction
        database_success = create_evidence_files_atomic(files_to_create, job_id, PROCESS_NAME)
        if not database_success:
            raise Exception("Database operations failed")

        oversize_file_sqs_queue_name = config['transfer_exception_queue_url'] #used to be config['oversize_download_queue_url'] change after phase 1

        # Step 2: Send SQS messages after successful database operations
        sqs_results = send_batch_sqs_messages(
            normal_sqs_messages,
            oversize_sqs_messages,
            config['normal_download_queue_url'],
            oversize_file_sqs_queue_name
        )

        # Step 3: Update job status
        lambda_update_job_status(
            job_id=job_id,
            status_value="DOWNLOAD-READY",
            msg=f"Queued {len(files_to_create)} files for download"
            
        )

        logger.log_success( event=Constants.PROCESS_NAME,message= json.dumps({
                'message': 'Evidence processing completed successfully',
                'job_id': job_id,
                'source_case_id': source_case_id,
                'files_processed': len(files_to_create),
                'normal_files': len(normal_sqs_messages),
                'oversize_files': len(oversize_sqs_messages)
            }), job_id=job_id
           
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Evidence processing completed successfully',
                'job_id': job_id,
                'source_case_id': source_case_id,
                'environment': env_stage,
                'files_processed': len(files_to_create),
                'normal_files': len(normal_sqs_messages),
                'oversize_files': len(oversize_sqs_messages),
                'sqs_results': sqs_results
            })
        }

    except botocore.exceptions.ClientError as e:
        logger.log_error(event=Constants.PROCESS_NAME,error=e )
        send_exception_message(
            exception_queue_url, job_id, None, source_case_id, f"AWS service error: {str(e)}"
        )
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'AWS service error: {str(e)}'})
        }
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME,error=e )
        send_exception_message(
            exception_queue_url, job_id, None, source_case_id, f"Processing error: {str(e)}"
        )
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Processing failed: {str(e)}'})
        }
def process_case_evidence_with_sqs(job_id: str, source_case_id: str,  context_data: Dict[str, Any]):
    config = get_lambda_config(context_data=context_data)
    evidence_list = retrieve_case_evidence(source_case_id, job_id, config, context_data)
    files_to_create, normal_sqs_messages, oversize_sqs_messages = process_evidence_records(
        evidence_list, job_id, source_case_id, config
    )
    return persist_and_queue(
        files_to_create, normal_sqs_messages, oversize_sqs_messages, job_id, source_case_id, config
    )

def retrieve_case_evidence(source_case_id: str, job_id : str, config: Dict[str, str], context_data: Dict[str, Any]) -> List[Dict]:
    """Fetch evidence list from Axon API."""
    logger.log_success(event="retrieve case evidence", message="calling retrieve case evidence")
    evidence_list = get_case_evidence_from_api(source_case_id, job_id,  config, context_data=context_data)
    logger.log_success(event=Constants.PROCESS_NAME, message=json.dumps({
            'message': 'Retrieved evidence list',
            'source_case_id': source_case_id,
            'evidence_count': len(evidence_list)
        }), job_id=job_id
    )
    return evidence_list

def get_evidence_details_from_api(evidence_id: str, config: Dict[str, str]) -> Dict:
    """Get evidence details from Axon API using configuration from SSM."""
    import requests
    
    try:
        headers = {
            'Authorization': f'Bearer {config["axon_bearer_token"]}',
            'Content-Type': 'application/json'
        }
        
        # Build the API URL
        api_url =  config['axon_base_url'] + 'api/v2/agencies/' + config['axon_agency_id'] + '/evidence/' + evidence_id 
        
        logger.log_success(event=Constants.PROCESS_NAME, message=f"Calling Axon Evidence Details API for evidence {evidence_id}")
        response = requests.get(api_url, headers=headers,  timeout=30,verify=True)
        response.raise_for_status()
        
       # Parse JSON response
        evidence_details = response.json()
        if not isinstance(evidence_details, dict):
            raise ValueError("API response is not a valid JSON object")

        
        logger.log_success(event=Constants.PROCESS_NAME,message=f"Retrieved evidence details for {evidence_id}")
        
        return evidence_details
        
    except requests.exceptions.RequestException as e:
        logger.log_error(event=Constants.PROCESS_NAME,error= e)
        raise Exception(f"Axon Evidence Details API error: {str(e)}")
    
    except ValueError as e:
        logger.log_error(
            event=Constants.PROCESS_NAME,
            error=f"Invalid response for evidence {evidence_id}: {str(e)}",
            url=api_url
        )
        raise
    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=e)
        raise


def process_evidence_records(
    evidence_list: List[Dict], job_id: str, source_case_id: str, config: Dict[str, str]
) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """Process evidence records and prepare file data and SQS messages."""
    files_to_create = []
    normal_sqs_messages = []
    oversize_sqs_messages = []
    
    for evidence_record in evidence_list:
        evidence_id = evidence_record['id']
        if get_evidence_file(evidence_id):
            print(f"Skipping existing evidence: {evidence_id}")
            continue
        try:
            evidence_details = get_evidence_details_from_api(evidence_id, config)
            file_size_bytes = evidence_details.get('size', 0)
            if file_size_bytes > Constants.SIZE_THRESHOLD_BYTES:
                custom_metadata = {
                    "file size" : file_size_bytes,
                    "evidence_id" : evidence_id,
                    "msg" : "transfer rejected; oversize evidence file detected"
                }
                error_message = "transfer rejected; oversize evidence file detected"
                logger.log_success(event=Constants.PROCESS_NAME, message=f"transfer rejected; oversize evidence file detected for job_id: {job_id}, source_case_id: {source_case_id}", job_id=job_id, custom_metadata=custom_metadata)
                lambda_update_job_status(job_id, StatusCodes.FAILED,error_message )
                db_manager.update_job_counts(job_id,len(evidence_record),last_modified_process="lambda: axon case detail and evidence filter")
                
                exception_data = {
                'job_id': job_id,
                'source_case_id' : source_case_id
                }

                sqs = boto3.client('sqs')
                sqs.send_message(
                QueueUrl=config['transfer_exception_queue_url'],
                MessageBody=json.dumps(exception_data),
                MessageGroupId=f"exceptions-{job_id}" if job_id else "exceptions-general",
                MessageDeduplicationId=f"{job_id}-{evidence_id}-{hash(error_message)}" if evidence_id else f"{job_id}-general-{hash(error_message)}"
                )

            else:
                state_code = (
                StatusCodes.DOWNLOAD_READY
                )

            files_data = evidence_details.get("data", {}).get("relationships", {}).get("files", {}).get("data", [])

            if not isinstance(files_data, list):
                
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Files field is not a list for evidence_id {evidence_id}: {files_data}"))
                continue
           
            for file_entry in files_data:
                 # Extract file attributes
                file_attributes = file_entry.get("attributes", {})
                if not isinstance(file_attributes, list):
                    logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Files attributes is not a list for evidence_id {evidence_id}: {file_attributes}"))
                    continue
                # Validate required file fields
                file_id = file_entry.get('id')
                file_size = file_attributes.get('size', 0)
                file_name = file_attributes.get('fileName', None)
                if not file_id or not isinstance(file_size, (int, float)):
                    logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Missing or invalid file id/size for evidence_id {evidence_id}: {file_entry}"))
                    continue

                file_data={
                    'evidence_id': evidence_id,
                    'job_id': job_id,
                    'evidence_transfer_state_code': state_code,
                    'evidence_file_id': file_id,
                    'evidence_file_type': file_attributes.get('contentType', 'unknown'),
                    'source_case_id': source_case_id,
                    'file_size_bytes': file_size,
                    'checksum': file_attributes.get('checksum'),
                    'last_modified_process': Constants.PROCESS_NAME,
                    'bridge_s3_cleanup_scheduled_date' : '',
                    'bridge_s3_cleanup_scheduled' : False,
                    'bridge_s3_cleanup_completed' : False,
                    'bridge_s3_cleanup_completed_date' : '',
                    'axon_evidence_category_id' : 0,
                    'evidence_file_name' : file_name
                    }
                files_to_create.append(file_data)
            
            sqs_message = create_sqs_message(job_id, evidence_id, source_case_id, evidence_details)
            (normal_sqs_messages if state_code == StatusCodes.DOWNLOAD_READY else oversize_sqs_messages).append(sqs_message)
        except Exception as e:
            handle_error_and_queue(
                e, config['transfer_exception_queue_url'], job_id, evidence_id, source_case_id, "evidence_processing"
            )
    
    return files_to_create, normal_sqs_messages, oversize_sqs_messages

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
    
        # If message_attributes is a string (e.g., JSON), parse it to a dict
        if isinstance(message_attributes, str):
            try:
                message_attributes = json.loads(message_attributes)
            except json.JSONDecodeError:
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception("Invalid JSON in messageAttributes"))
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                    'error': 'Invalid JSON in messageAttributes',
                    'received_event': event
                })
            }
    
        # Ensure it's a dict after any parsing
        if not isinstance(message_attributes, dict):
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("messageAttributes must be a dict"))
            return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'messageAttributes must be a dict',
                'received_event': event
            })
        }
    
        job_id = None
        source_case_id = None

        if 'job_id' in message_attributes:
            attr = message_attributes['job_id']
            # If attr is already a string (non-standard attribute), use it directly
        if isinstance(attr, str):
            job_id = attr
        elif isinstance(attr, dict):
            job_id = attr['stringValue'] if attr.get('dataType') == 'String' else attr.get('binaryValue')
        else:
            # Handle unexpected types
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Unexpected type for job_id attribute: {type(attr)}"))

        if 'source_case_id' in message_attributes:
            attr = message_attributes['source_case_id']
        if isinstance(attr, str):
            source_case_id = attr
        elif isinstance(attr, dict):
            source_case_id = attr['stringValue'] if attr.get('dataType') == 'String' else attr.get('binaryValue')
        else:
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Unexpected type for source_case_id attribute: {type(attr)}"))

        if not job_id or not source_case_id:
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("job_id and source_case_id are required"))
            return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'job_id and source_case_id are required',
                'received_event': event
            })
            }

        logger.log_success(event=Constants.PROCESS_NAME, message=f"Processing evidence for job_id: {job_id}, source_case_id: {source_case_id}, environment: {env_stage}", job_id=job_id)
        results = process_case_evidence_with_sqs(job_id, source_case_id, context_data=base_context)

        logger.log_success(
            event="Case Detail Evidence Filter End",
            message="Successfully completed AxonCaseDetailEvidenceFilter execution",
            job_id=context.aws_request_id
        )
        return results

    except Exception as e:
        logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Lambda execution failed: {str(e)}"))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Lambda execution failed: {str(e)}'})
        }