import json
import boto3
import urllib3
import urllib.parse
import os
import time
import botocore.exceptions

from datetime import datetime, timezone, timedelta
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager

class Constants:
    IN_PROGRESS = "IN_PROGRESS",
    ERROR = "ERROR"

def initialize_aws_clients(env_stage: str) -> tuple:
    """Initialize AWS clients and resources with custom configuration."""
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "standard"})
    return (
        boto3.client("ssm", region_name="ca-central-1", config=config),
        boto3.client('sqs', region_name="ca-central-1", config=config),
        boto3.resource("dynamodb", region_name="ca-central-1").Table("agency-lookups")
    )

def initialize_http_pool() -> urllib3.PoolManager:
    """Initialize HTTP connection pool with retries and timeout."""
    return urllib3.PoolManager(
        timeout=urllib3.Timeout(connect=5.0, read=10.0),
        retries=urllib3.Retry(total=3, backoff_factor=1)
    )

def get_ssm_parameters(ssm_client, env_stage: str, logger: LambdaStructuredLogger, job_id: str) -> dict:
    """Retrieve and process SSM parameters."""
    parameter_names = [
        f'/{env_stage}/isl_endpoint_url',
        f'/{env_stage}/isl_endpoint_secret',
        f'/{env_stage}/isl_endpoint/case_id_method',
        f'/{env_stage}/axon/api/client_id'
    ]
    
    try:
        ssm_response = ssm_client.get_parameters(Names=parameter_names, WithDecryption=True)
        parameters = {param['Name']: param['Value'] for param in ssm_response['Parameters']}
        
        if ssm_response.get('InvalidParameters'):
            logger.log_error(
                event="SSM Param Retrieval",
                error=f"Failed to retrieve parameters: {ssm_response['InvalidParameters']}",
                job_id=job_id
            )
            raise ValueError(f"Failed to retrieve some parameters: {ssm_response['InvalidParameters']}")
        
        logger.log_success(
            event="SSM Param Retrieval",
            message="Parameters collected successfully",
            job_id=job_id,
            custom_metadata={"parameter_names": parameter_names}
        )
        return parameters
    except Exception as e:
        logger.log_error(event="SSM Param Retrieval Failed", error=str(e), job_id=job_id)
        raise

def receive_sqs_messages(sqs_client, queue_name: str, logger: LambdaStructuredLogger, job_id: str) -> list:
    """Receive messages from SQS queue."""
    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        sqs_response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            VisibilityTimeout=30,
            MessageAttributeNames=['All']
        )
        return sqs_response.get('Messages', [])
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"ClientError receiving message: {error_code} - {str(e)}")
        raise  # Re-raise to trigger retry
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise  # Re-raise non-retryable errors

def parse_message_attributes(message: dict) -> tuple:
    """Parse job_id and case_title from SQS message attributes."""
    body = message.get('MessageAttributes', {})
    job_id = body.get("Job_id", {}).get('StringValue')
    case_title = body.get("Source_case_title", {}).get('StringValue')
    carJurId = body.get("cadJurId", {}).get('StringValue')
    
    if not job_id or not case_title or not carJurId :
        raise ValueError("Missing job_id or Source_case_title or cadJurId in message")
    
    return job_id, case_title, carJurId

def parse_case_title(case_title: str) -> tuple:
    """Parse agency and file number from case title."""
    parts = case_title.split('-')
    if len(parts) < 2:
        raise ValueError("Invalid case_title format")
    
    return parts[0].strip(), '-'.join(parts[1:]).strip()

def lookup_agency_code(agency_code_table, rms_jur_id: str, cadJurId:str, logger: LambdaStructuredLogger, job_id: str) -> tuple:
    """Lookup agency code in DynamoDB."""
    logger.log(event="Agency Code Lookup", status=LogStatus.IN_PROGRESS, message="Retrieving agency code...", job_id=job_id)
    
    dynamo_response = agency_code_table.get_item(Key={'rmsJurId ': rms_jur_id})
    item = dynamo_response.get('Item')
    
    if not item:
        """ Try again with cadJurId"""
        dynamo_response = agency_code_table.query(IndexName="cadJurId-GSI",
                                            KeyConditionExpression="#cadJurId = :cadJurId_val", 
                                            ExpressionAttributeNames={"#cadJurId": "cadJurId"},  # Avoid reserved word issues,
                                            ExpressionAttributeValues={":cadJurId_val": cadJurId})
        dynamo_response = agency_code_table.get_item(Key={'cadJurId ': cadJurId})
        item = dynamo_response.get('Item')
        if not item:
            logger.log_error(event="Agency Code Lookup Failed", error=f"No item found for rmsJurId : {rms_jur_id} or cadJurid: {cadJurId} ", job_id=job_id)
            return None, None, None
       
        logger.log(event="Axon Case Agency Lookup", status=LogStatus.IN_PROGRESS, message=f"Agency prefix lookup successful", job_id=job_id)
        return None, None, None
    
    return (
        item.get('bcpsAgencyIdCode', ''),
        item.get('subAgencyYN', 'N'),
        item.get('subAgencies', [])
    )

def call_dems_api(http: urllib3.PoolManager, dems_api_url: str, bearer_token: str, agency_code: str,
                 agency_file_number: str, logger: LambdaStructuredLogger, job_id: str) -> tuple:
    """Call DEMS API and return response status and data."""
    headers = {
        'Authorization': f"Bearer {bearer_token}",
        'agencyIdCode': agency_code,
        'agencyFileNumber': agency_file_number
    }
    
    logger.log(event="DEMS API Call", status=LogStatus.IN_PROGRESS, message=f"Calling DEMS API with agencyIdCode: {agency_code}", job_id=job_id)
    
    start_time = time.perf_counter()
    api_response = http.request('GET', dems_api_url, headers=headers)
    response_time = time.perf_counter() - start_time
    
    logger.log_api_call(
        event="DEMS ISL Get Cases",
        url=dems_api_url,
        method="GET",
        status_code=api_response.status,
        response_time=response_time,
        job_id=job_id
    )
    
    return api_response.status, api_response.data.decode('utf-8').strip()

def update_job_status(db_manager: DatabaseManager, job_id: str, status_value: str, logger: LambdaStructuredLogger):
    """Update job status in the database."""
    update_job_status = db_manager.get_status_code_by_value(value=status_value)
    if update_job_status:
        status_identifier = str(update_job_status["identifier"])
        db_manager.update_job_status(
            job_id=job_id,
            status_code=status_identifier,
            job_msg="",
            last_modified_process="lambda: rcc and dems case validator"
        )

def send_sqs_message(sqs_client, queue_name: str, job_id: str, case_id: str,
                    logger: LambdaStructuredLogger, current_timestamp: str, custom_exception: Exception= None, case_title: str=None):
    """Send message to SQS queue."""
    try:
        queue_url = sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
        logger.log(event="calling SQS to add msg", status=LogStatus.IN_PROGRESS, message="Trying to call SQS ...")
        
        message_attributes = {
            'Job_id': {'DataType': 'String', 'StringValue': job_id},
            'Source_case_id': {'DataType': 'String', 'StringValue': case_id}
        }
        # Add exception message to message attributes if custom_exception is provided
        if custom_exception:
            message_attributes['ExceptionMessage'] = {
                'DataType': 'String',
                'StringValue': str(custom_exception)[:256]  # SQS message attributes have a 256-byte limit
            }
        # Add case_title to message attributes if case_title is not None
        if case_title is not None:
            message_attributes['Case_title'] = {
                'DataType': 'String',
                'StringValue': case_title[:256],  # Ensure case_title adheres to SQS 256-byte limit
                'Job_id': {'DataType': 'String', 'StringValue': job_id}
            }

        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody='Sending SQS message to ' + queue_name,
            DelaySeconds=0,
            MessageGroupId="axon-evidence-transfer",
            MessageDeduplicationId=job_id,
            MessageAttributes=message_attributes
        )
        
        logger.log_sqs_message_sent(
            queue_url=queue_url,
            message_id=response,
            message_body={
                "timestamp": current_timestamp,
                "level": "INFO",
                "function": "axonRccAndDemsCaseValidator",
                "event": "SQSMessageQueued",
                "message": "Queued message for Axon Case Detail and Evidence Filter",
                "job_id": job_id,
                "source_case_title": case_title,
                "additional_info": {
                    "target_queue": queue_name,
                    "message_group_id": job_id,
                    "deduplication_id": "file-" + response
                }
            }
        )
    except Exception as e:
        logger.log_error(event="SQS Message Send Failed", error=str(e), job_id=job_id)

def process_message(message: dict, parameters: dict, http: urllib3.PoolManager, agency_code_table,
                   db_manager: DatabaseManager, sqs_client, logger: LambdaStructuredLogger):
    """Process a single SQS message."""
    try:
        # Get environment stage from environment variable
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        job_id, case_title, cadJurId = parse_message_attributes(message)
        rms_jur_id, agency_file_number = parse_case_title(case_title)
        
        agency_id_code, sub_agency_yn, sub_agencies = lookup_agency_code(agency_code_table, rms_jur_id, cadJurId, logger, job_id)
        if not agency_id_code:
            status_value =  "INVALID-AGENCY-IDENTIFIER"
            update_job_status(db_manager, job_id, status_value, logger)
            send_sqs_message(sqs_client, 'q-transfer-exception', job_id, case_title, logger, current_timestamp, Exception("Agency Code not found"), case_title)
            return
        
        logger.log(
            event="Axon Case Agency Lookup",
            status=LogStatus.IN_PROGRESS,
            message="Agency prefix lookup successful",
            job_id=job_id,
            custom_metadata={
                "rms_jur_id": rms_jur_id,
                "agency_id_code": agency_id_code,
                "agency_file_number": agency_file_number
            }
        )
        
        dems_api_url = parameters[f'/{env_stage}/isl_endpoint_url'] + parameters[f'/{env_stage}/isl_endpoint/case_id_method']
        bearer_token = parameters[f'/{env_stage}/isl_endpoint_secret']
        agency_codes_to_try = [agency_id_code] + (sub_agencies if sub_agency_yn == 'Y' else [])
        found_dems_case = False
        for code in agency_codes_to_try:
            status, dems_case_id = call_dems_api(http, dems_api_url, bearer_token, code, agency_file_number, logger, job_id)
            
            if status == 200 and dems_case_id:
                db_manager.set_dems_case(job_id, dems_case_id, "Verify Rcc Dems Case")
                logger.log_success(event="DEMS Case Found", message=f"DEMS case ID: {dems_case_id}", job_id=job_id)
                found_dems_case = True
                break
            elif status >= 400:
                logger.log_error(event="DEMS API Error", error=f"HTTP error: {status}", job_id=job_id)
        
        status_value = "VALID-CASE" if found_dems_case else "INVALID-AGENCY-IDENTIFIER"
        update_job_status(db_manager, job_id, status_value, logger)
        
        if found_dems_case:
            current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
            send_sqs_message(sqs_client, 'q-case-detail.fifo', job_id, dems_case_id, logger, current_timestamp)
            
             # If processing succeeds, delete the message
            queue_url = sqs_client.get_queue_url(QueueName="q-case-found.fifo")['QueueUrl']
            sqs_client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        logger.info(f"Deleted message: {message['MessageId']}")
        
        if not found_dems_case:
            logger.log(
                event="axonRccAndDemsCaseValidator",
                status= Constants.ERROR,
                message="Agency prefix lookup unsuccessful - not matched",
                job_id=job_id,
                additional_info={"rms_jur_id": rms_jur_id, "agencyFileNumber": agency_file_number}
            )
            send_sqs_message(sqs_client, 'q-transfer-exception', job_id, case_title, logger, current_timestamp, Exception("Case not found"), case_title)
            
    except Exception as msg_err:
        logger.log_error(event="Message Processing Failed", error=str(msg_err), job_id=job_id)

def lambda_handler(event, context):
    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

    """Main Lambda handler function."""
    logger = LambdaStructuredLogger()
    logger.log_start(event="Verify Dems Case Start", job_id=context.aws_request_id)
    
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')
    ssm_client, sqs_client, agency_code_table = initialize_aws_clients(env_stage)
    db_manager = get_db_manager(env_param_in=env_stage)
    db_manager._initialize_pool()
    http = initialize_http_pool()
    
    try:
        parameters = get_ssm_parameters(ssm_client, env_stage, logger, context.aws_request_id)
        messages = receive_sqs_messages(sqs_client, 'q-case-found.fifo', logger, context.aws_request_id)
        
        if not messages:
            logger.log(event="SQS Poll", status=Constants.IN_PROGRESS, message="No messages in queue", job_id=context.aws_request_id)
            return {'statusCode': 200, 'body': 'No messages to process'}
        
        for message in messages:
            process_message(message, parameters, http, agency_code_table, db_manager, sqs_client, logger)
        
        logger.log_success(
            event="Verify Dems Case End",
            message="Successfully completed axonRccAndDemsCaseValidator execution",
            job_id=context.aws_request_id
        )
        return {'statusCode': 200, 'body': 'Processing complete'}
    
    except Exception as e:
        logger.log_error(event="Lambda Execution Failed", error=str(e), job_id=context.aws_request_id)
        raise