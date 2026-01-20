import json
import boto3
import urllib3
import urllib.parse
import os
import time
import botocore.exceptions
import random
import string

from datetime import datetime, timezone, timedelta
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager


class Constants:
    IN_PROGRESS = "IN_PROGRESS"
    ERROR = "ERROR",
    PROCESS_NAME = "axonRccAndDemsCaseValidator"
    HTTP_OK = 200
    HTTP_BAD_REQUEST = 400



class DemsCaseValidator:
    """Main class encapsulating the DEMS case validation logic for Lambda processing."""

    def __init__(self, env_stage: str, logger: LambdaStructuredLogger, job_id: str):
        """Initialize the validator with environment stage, logger, and job ID."""
        self.env_stage = env_stage
        self.logger = logger
        self.job_id = job_id
        self.ssm_client, self.sqs_client, self.agency_code_table = self._initialize_aws_clients()
        self.db_manager = get_db_manager(env_param_in=env_stage)
        self.db_manager._initialize_pool()
        self.http = self._initialize_http_pool()
        self.parameters = self._get_ssm_parameters()

    def _initialize_aws_clients(self) -> tuple:
        """Initialize AWS clients and resources with custom configuration."""
        config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "standard"})
        return (
            boto3.client("ssm", region_name="ca-central-1", config=config),
            boto3.client('sqs', region_name="ca-central-1", config=config),
            boto3.resource("dynamodb", region_name="ca-central-1").Table("agency-lookups")
        )

    def _initialize_http_pool(self) -> urllib3.PoolManager:
        """Initialize HTTP connection pool with retries and timeout."""
        return urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=5.0, read=10.0),
            retries=urllib3.Retry(total=3, backoff_factor=1)
        )

    def _get_ssm_parameters(self) -> dict:
        """Retrieve and process SSM parameters."""
        parameter_names = [
            f'/{self.env_stage}/isl_endpoint_url',
            f'/{self.env_stage}/isl_endpoint_secret',
            f'/{self.env_stage}/isl_endpoint/case_id_method',
            f'/{self.env_stage}/axon/api/client_id',
            f'/{self.env_stage}/bridge/sqs-queues/lambda-rcc-dems-case-validator-retries'
        ]
        
        try:
            ssm_response = self.ssm_client.get_parameters(Names=parameter_names, WithDecryption=True)
            parameters = {param['Name']: param['Value'] for param in ssm_response['Parameters']}
            
            if ssm_response.get('InvalidParameters'):
                self.logger.log_error(
                    event="SSM Param Retrieval",
                    error=f"Failed to retrieve parameters: {ssm_response['InvalidParameters']}",
                    job_id=self.job_id
                )
                raise ValueError(f"Failed to retrieve some parameters: {ssm_response['InvalidParameters']}")
            
            self.logger.log_success(
                event="SSM Param Retrieval",
                message="Parameters collected successfully",
                job_id=self.job_id,
                custom_metadata={"parameter_names": parameter_names}
            )
            return parameters
        except Exception as e:
            self.logger.log_error(event="SSM Param Retrieval Failed", error=e, job_id=self.job_id)
            raise

    def receive_sqs_messages(self, queue_url: str) -> list:
        """Receive messages from SQS queue."""
        try:
           
            sqs_response = self.sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=30,
                MessageAttributeNames=['All']
            )
           
            return sqs_response.get('Messages', [])
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            self.logger.log_error(event="SQS retrieval failed", error=f"ClientError receiving message: {error_code} - {str(e)}",job_id=self.job_id)
            raise  # Re-raise to trigger retry
        except Exception as e:
            self.logger.log_error(event="Unexpected error in SQS Retrieval", error=f"Unexpected error: {str(e)}",job_id=self.job_id)
            raise  # Re-raise non-retryable errors

    @staticmethod
    def parse_message_attributes(message: dict) -> tuple:
        """Parse job_id and case_title from SQS message attributes."""
        body = message.get('messageAttributes', {})
       
        job_id = None
        case_title = None
        carJurId = None
        attempt_number_str  = None
        attenmpt_number = 1

        first_attempt_time= None
        last_attempt_time = None

        if 'job_id' in body:
            attr = body['job_id']
            if attr['dataType'] == 'String':
                job_id = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                job_id = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')
           

        #job_id = body.get("job_id", {}).get('StringValue')
        if 'source_case_title' in body:
            attr = body['source_case_title']
            if attr['dataType'] == 'String':
                case_title = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                case_title = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')

     
        if 'cadJurId' in body:
            attr = body['cadJurId']
            if attr['dataType'] == 'String':
                carJurId = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                carJurId = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')
        
        if not job_id or not case_title :
            raise ValueError("Missing job_id or source_case_title in messsage")
        
        if 'first_attempt_time' in body:
            attr = body['first_attempt_time']
            if attr['dataType'] == 'String':
                first_attempt_time = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                first_attempt_time = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')

        if 'last_attempt_time' in body:
            attr = body['last_attempt_time']
            if attr['dataType'] == 'String':
                last_attempt_time = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                last_attempt_time = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')

        if 'attempt_number' in body:
            attr = body['attempt_number']
            if attr['dataType'] == 'String':
                attempt_number_str = attr['stringValue']
            elif attr['dataType'] == 'Binary':
                attempt_number_str = attr.get('binaryValue')  # Decode if needed, e.g., job_id.decode('utf-8')
        
        if attempt_number_str:
            attenmpt_number = int(attempt_number_str)

        return job_id, case_title, carJurId, attenmpt_number, first_attempt_time, last_attempt_time

    @staticmethod
    def parse_case_title(case_title: str) -> tuple:
        """Parse agency and file number from case title."""
        parts = case_title.split('-')
        if len(parts) < 2:
            raise ValueError("Invalid case_title format")
        
        return parts[0].strip(), '-'.join(parts[1:]).strip()

    def lookup_agency_code(self, rms_jur_id: str, cadJurId: str) -> tuple:
        """Lookup agency code in DynamoDB."""
        self.logger.log(event="Agency Code Lookup", status=LogStatus.IN_PROGRESS, message="Retrieving agency code for rmsJurId : " + rms_jur_id, job_id=self.job_id)
    
        item = None
        try:
            # First attempt with rms_jur_id
            dynamo_response = self.agency_code_table.get_item(Key={'rmsJurId': rms_jur_id})
            item = dynamo_response.get('Item')
        
            if not item:
                # Try again with cadJurId
                if cadJurId:
                    dynamo_response = self.agency_code_table.get_item(Key={'cadJurId': cadJurId})
                    item = dynamo_response.get('Item')
                    
                if not item:
                    self.logger.log_error(event="Agency Code Lookup Failed", error=f"No item found for rmsJurId : {rms_jur_id} or cadJurid: {cadJurId} ", job_id=self.job_id)
                    return None, None, None
           
            # Item found (via either key); log success
            self.logger.log(event="Axon Case Agency Lookup", status=LogStatus.IN_PROGRESS, message=f"Agency prefix lookup successful", job_id=self.job_id)
        
            # Return extracted values
            return (
                item.get('bcpsAgencyIdCode', ''),
                item.get('subAgencyYN', 'N'),
                item.get('subAgencies', [])
            )
    
        except Exception as e:  # Broad catch for any DynamoDB-related errors (e.g., ClientError, NoCredentialsError)
            error_msg = f"DynamoDB lookup failed for rmsJurId: {rms_jur_id} or cadJurId: {cadJurId}. Error: {str(e)}"
            self.logger.log_error(event="Agency Code Lookup Failed", error=error_msg, job_id=self.job_id)
            return None, None, None

    def call_dems_api(self, dems_api_url: str, bearer_token: str, agency_code: str,
                      agency_file_number: str) -> tuple:
        """Call DEMS API and return response status and data."""
        headers = {
            'Authorization': f"Bearer {bearer_token}",
            'agencyIdCode': agency_code,
            'agencyFileNumber': agency_file_number
        }
        
        self.logger.log(event="DEMS API Call", status=LogStatus.IN_PROGRESS, message=f"Calling DEMS API with agencyIdCode: {agency_code}", job_id=self.job_id)
        try:
            start_time = time.perf_counter()
            api_response = self.http.request('GET', dems_api_url, headers=headers)
            response_time = time.perf_counter() - start_time
        
            self.logger.log_api_call(
            event="DEMS ISL Get Cases",
            url=dems_api_url,
            method="GET",
            status_code=api_response.status,
            response_time=response_time,
            job_id=self.job_id
            )
        
            return api_response.status, api_response.data.decode('utf-8').strip()
        except Exception as e:
            self.logger.log_error(event="Call to DEMS API Failed", error=e, job_id=self.job_id)
            return Constants.HTTP_BAD_REQUEST, None

    def update_job_status(self, job_id: str, status_value: str, agency_id_code:str, agency_file_number:str, job_msg:str, retry_count:int):
        """Update job status in the database."""
        if status_value:
            update_job_status = self.db_manager.get_status_code_by_value(value=status_value)
            if update_job_status:
                status_identifier = str(update_job_status["identifier"])
                self.db_manager.update_job_status(
                    job_id=job_id,
                    status_code=status_identifier,
                    job_msg=job_msg,
                    agency_id_code=agency_id_code,
                    agency_file_number=agency_file_number,
                    retry_count=retry_count,
                    last_modified_process="lambda: rcc and dems case validator"
                )

    def send_sqs_message(self, queue_name: str, job_id: str, case_id: str,
                         current_timestamp: str, custom_exception: Exception = None, case_title: str = None, message_attributes:dict=None):
        """Send message to SQS queue."""
        # Define the alphanumeric character pool
        alphanumeric_chars = string.ascii_letters + string.digits

        # Generate a random string of 20 characters
        random_string = ''.join(random.choices(alphanumeric_chars, k=20))

        try:
            queue_url = self.sqs_client.get_queue_url(QueueName=queue_name)['QueueUrl']
            self.logger.log(event="calling SQS to add msg", status=LogStatus.IN_PROGRESS, message="Trying to call SQS ...")
            if not message_attributes:
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
            if case_title is not None and message_attributes is not None:
                message_attributes['Case_title'] = {
                    'DataType': 'String',
                    'StringValue': case_title[:256],  # Ensure case_title adheres to SQS 256-byte limit
                }

            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody='Sending SQS message to ' + queue_name,
                DelaySeconds=0,
                MessageGroupId="axon-evidence-transfer",
                MessageDeduplicationId=job_id,
                MessageAttributes=message_attributes
            )
            
            self.logger.log_sqs_message_sent(
                queue_url=queue_url,
                message_id=response,
                response_time_ms=1,
                message_body={
                    "timestamp": current_timestamp,
                    "level": "INFO",
                    "function": "axonRccAndDemsCaseValidator",
                    "event": "SQSMessageQueued",
                    "message": "Queued message for Axon Case Detail and Evidence Filter",
                    "job_id": job_id,
                    "source_case_title": case_title,
                    "message_attributes": {
                        "target_queue": queue_name,
                        "message_group_id": job_id,
                        "deduplication_id": "file-" + random_string
                    }
                }
            )
        except Exception as e:
            self.logger.log_error(event="SQS Message Send Failed", error=e, job_id=self.job_id)

    def process_message(self, message: dict):
        """Process a single SQS message."""
        try:
            job_id, case_title, cadJurId, attenmpt_number, first_attempt_time, last_attempt_time = self.parse_message_attributes(message)
            rms_jur_id, agency_file_number = self.parse_case_title(case_title)
            
            agency_id_code, sub_agency_yn, sub_agencies = self.lookup_agency_code(rms_jur_id, cadJurId)
            if not agency_id_code:
                #couldn't find agency code, send exception message
                status_value = "INVALID-AGENCY-IDENTIFIER"
                self.update_job_status(job_id, status_value, agency_id_code=agency_id_code, agency_file_number=agency_file_number,job_msg="",retry_count=attenmpt_number+1)
                current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                queue_url = self.sqs_client.get_queue_url(QueueName="q-case-found.fifo")['QueueUrl']
                self.sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['receiptHandle']
                )
                self.logger.log( event=Constants.PROCESS_NAME, status=LogStatus.IN_PROGRESS, message=f"Deleted message: {message['receiptHandle']}", job_id=job_id)
                self.send_sqs_message('q-transfer-exception.fifo', job_id, case_title, current_timestamp, Exception("Agency Code not found"), case_title)
                return
            
            self.logger.log(
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
            
            dems_api_url = self.parameters[f'/{self.env_stage}/isl_endpoint_url'] + self.parameters[f'/{self.env_stage}/isl_endpoint/case_id_method']
            bearer_token = self.parameters[f'/{self.env_stage}/isl_endpoint_secret']
            agency_codes_to_try = [agency_id_code] + (sub_agencies if sub_agency_yn == 'Y' else [])
            found_dems_case = False

            for code in agency_codes_to_try:
                status, dems_case_id = self.call_dems_api(dems_api_url, bearer_token, code, agency_file_number)
                
                if status == Constants.HTTP_OK and dems_case_id:
                    self.db_manager.set_dems_case(job_id, dems_case_id, "Verify Rcc Dems Case")
                    self.logger.log_success(event="DEMS Case Found", message=f"DEMS case ID: {dems_case_id}", job_id=job_id)
                    found_dems_case = True
                    break
                elif status >= Constants.HTTP_BAD_REQUEST:
                    self.logger.log_error(event="DEMS API Error", error=f"HTTP error: {status}", job_id=job_id)
            
            if sub_agency_yn == 'N' :
                lambda_rcc_dems_case_retries = self.parameters[f'{self.env_stage}/bridge/sqs-queues/lambda-rcc-dems-case-validator-retries']
                if int(lambda_rcc_dems_case_retries) < attenmpt_number:
                    #no case found path
                    self.process_no_case_found(job_id, agency_id_code, agency_file_number,"dems case not found. Retrying",attenmpt_number,first_attempt_time, message['receiptHandle'], case_title)
                elif int(lambda_rcc_dems_case_retries) == attenmpt_number:
                    # exception path
                    self.process_exception_message(job_id, rms_jur_id, agency_id_code,agency_file_number, message['receiptHandle'], case_title)

            status_value = "VALID-CASE" if found_dems_case else None
            self.update_job_status(job_id, status_value, agency_file_number=agency_file_number, agency_id_code=agency_id_code, retry_count=attenmpt_number,job_msg="")
            
            current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
            if found_dems_case:
                self.send_sqs_message('q-axon-case-share-received.fifo', job_id,dems_case_id, current_timestamp)
                self.send_sqs_message('q-case-detail.fifo', job_id, dems_case_id, current_timestamp)
                
                # If processing succeeds, delete the message
                queue_url = self.sqs_client.get_queue_url(QueueName="q-case-found.fifo")['QueueUrl']
                self.sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['receiptHandle']
                )
                self.logger.info(f"Deleted message: {message['MessageId']}")
            
            if not found_dems_case:
                self.logger.log(
                    event="axonRccAndDemsCaseValidator",
                    status=Constants.ERROR,
                    message="Agency prefix lookup unsuccessful - not matched",
                    job_id=job_id,
                    custom_metadata={"rms_jur_id": rms_jur_id, "agencyFileNumber": agency_file_number}
                )
                self.send_sqs_message('q-transfer-exception.fifo', job_id, case_title, current_timestamp, Exception("Case not found"), case_title)
                
        except Exception as msg_err:
            self.logger.log_error(event="Message Processing Failed", error=str(msg_err), job_id=self.job_id)

def process_no_case_found(self , job_id:str, rms_jur_id:str, agency_id_code:str,agency_file_num:str, job_msg:str,retry_count:int, first_attempt_time:str, message_handle:str, source_case_title:str )->bool:
        status_value = "INVALID-CASE"
        self.update_job_status(job_id,status_value , agency_file_num, agency_id_code, job_msg=job_msg, retry_count = retry_count +1 )

        current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        self.logger.log(
                    event="Axon Case Agency Lookup",
                    status=Constants.INFO,
                    function="axonRccAndDemsCaseValidator",
                    message="Agency prefix lookup to RCC and DEMS usuccessful - no match",
                    source_case_title=source_case_title,
                    job_id=job_id,
                    custom_metadata={"rms_jur_id": rms_jur_id, "agencyFileNumber": agency_file_num, "agency_id_code" : agency_id_code}
                )
          # end of attempts, delete the message
        queue_url = self.sqs_client.get_queue_url(QueueName="q-case-found.fifo")['QueueUrl']
        self.sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message_handle
                )
        self.logger.info(f"Deleted message: {message_handle}")
        message_attributes = {
            "attempt_number"        : retry_count,
            "first_attempt_time"    : first_attempt_time,
            "last_attempt_time"     : current_timestamp

        }
        self.send_sqs_message('q-axon-case-found.fifo', job_id, case_title=source_case_title, current_timestamp=current_timestamp, message_attributes=message_attributes)

def process_exception_message(self, job_id:str, rms_jur_id:str, agency_id_code:str,agency_file_num:str,message_handle:str, source_case_title:str )->bool:
    status_code = "INVALID-AGENCY-IDENTIFIER"
    self.update_job_status(job_id,status_code, agency_file_num, agency_id_code,job_msg=None)
    current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
    self.logger.log(
                    function="axonRccAndDemsCaseValidator",
                    event="Axon Case Agency Lookup",
                    status=Constants.ERROR,
                    message="Agency prefix lookup unsuccessful - not matched",
                    job_id=job_id,
                    custom_metadata={"rms_jur_id": rms_jur_id, "agencyFileNumber": agency_file_num}

                )
    queue_url = self.sqs_client.get_queue_url(QueueName="q-case-found.fifo")['QueueUrl']
    self.sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message_handle
                )
    self.logger.info(f"Deleted message: {message_handle}")
    message_attributes = {
            "job_id"                        : job_id,
            "source_case_title"             : source_case_title,
            "exception_agency_match"        : current_timestamp

        }
    self.send_sqs_message('q-transfer-exception.fifo', job_id, None, current_timestamp, Exception("Case not found"), None, message_attributes=message_attributes)

def lambda_handler(event, context):
    """Main Lambda handler function."""
    # Get environment stage from environment variable
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')

    logger = LambdaStructuredLogger()
    logger.log_start(event="Verify Dems Case Start", job_id=context.aws_request_id)
    
    try:
        validator = DemsCaseValidator(env_stage, logger, context.aws_request_id)
        if not event.get("Records"):
            logger.log_error(event=Constants.PROCESS_NAME, error=Exception("No records found in event"))
            return {
                'statusCode': Constants.HTTP_BAD_REQUEST,
                'body': json.dumps({'error': 'No records found in event'})
            }

        for record in event["Records"]:
            message_attributes = record.get('messageAttributes', {})
           
            job_id = None

            if 'job_id' in message_attributes:
                attr = message_attributes['job_id']
                job_id = attr['stringValue'] if attr['dataType'] == 'String' else attr.get('binaryValue')

            if not job_id :
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception("job_id is required"))
                return {
                    'statusCode': Constants.HTTP_BAD_REQUEST,
                    'body': json.dumps({
                        'error': 'job_id and source_case_id are required',
                        'received_event': event
                    })
                }

            logger.log_success(event=Constants.PROCESS_NAME, message=f"Processing evidence for job_id: {job_id},  environment: {env_stage}", job_id=job_id)
            validator.process_message(record)

        
        if not event["Records"]:
            logger.log(event="SQS Poll", status=Constants.IN_PROGRESS, message="No messages in queue", job_id=context.aws_request_id)
            return {'statusCode': Constants.HTTP_OK, 'body': 'No messages to process'}
        
        logger.log_success(
            event="Verify Dems Case End",
            message="Successfully completed axonRccAndDemsCaseValidator execution",
            job_id=context.aws_request_id
        )
        return {'statusCode': Constants.HTTP_OK, 'body': 'Processing complete'}
    
    except Exception as e:
        logger.log_error(event="Lambda Execution Failed", error=e, job_id=context.aws_request_id)
        raise