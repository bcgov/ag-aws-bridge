import json
from typing import List, Optional, Tuple
import boto3
import urllib3
from urllib3.exceptions import MaxRetryError, NewConnectionError, TimeoutError
import os
import time
import botocore.exceptions
import random
import string

from datetime import datetime
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager


class Constants:
    IN_PROGRESS = "IN_PROGRESS"
    ERROR = "ERROR",
    PROCESS_NAME = "demsImportRequester"
    REGION_NAME = "ca-central-1"
    AGENCY_LOOKUP_TABLE_NAME = "agency-lookups"
    HTTP_OK_CODE = 200,
    IMPORT_REQUESTED= "IMPORT-REQUESTED"

class DemsImportRequester:
    """Main class to call the EDT create-load-file-import API to initiate "import" of evidence within EDT's S3 bucket to the particular, relevant target DEMS case"""

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
            boto3.client("ssm", region_name=Constants.REGION_NAME, config=config),
            boto3.client('sqs', region_name=Constants.REGION_NAME, config=config),
            boto3.resource("dynamodb", region_name=Constants.REGION_NAME).Table(Constants.AGENCY_LOOKUP_TABLE_NAME)
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
            f'/{self.env_stage}/edt/api/bearer',
            f'/{self.env_stage}/edt/api/import_url',
            f'/{self.env_stage}/edt/api/import_template',
            f'/{self.env_stage}/axon/api/client_id',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import-status',
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
            self.logger.log_error(event="SSM Param Retrieval Failed", error=str(e), job_id=self.job_id)
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
        """Parse required fields from SQS message attributes with case-insensitive lookup."""
        attrs = message.get('messageAttributes', {})

        def get_attr(name):
        # Try exact match first
            if name in attrs:
                val = attrs[name]
            else:
                # Try case-insensitive fallback
                lower_name = name.lower()
                for key, val in attrs.items():
                    if key.lower() == lower_name:
                        return val.get('stringValue') or val.get('binaryValue')
                return None
        
            return val.get('stringValue') or val.get('binaryValue')

        job_id = get_attr('job_id')
        sourcePath = get_attr('sourcePath') or get_attr('SourcePath') or get_attr('source_path')
        dems_case_id = get_attr('dems_case_id')
        destinationPath = get_attr('destinationPath') or get_attr('DestinationPath')

        required = {
            "job_id": job_id,
            "sourcePath": sourcePath,
            "dems_case_id": dems_case_id,
            "destinationPath": destinationPath,
        }

        missing = [k for k, v in required.items() if not v]
        if missing:
            
            raise ValueError(f"Missing required field(s): {', '.join(missing)}")

        return job_id, sourcePath, dems_case_id, destinationPath

    def callEDTDemsApi( self, job_id, dems_case_id, imagePath)->str:
        import_id = None
        try:
            api_url = self.parameters[f'/{self.env_stage}/edt/api/import_url']
            api_url = api_url.replace("$$$$", dems_case_id)
           
            headers = {
            'Authorization': f"Bearer {self.parameters[f'/{self.env_stage}/edt/api/bearer']}",
            "Accept" : "application/json"
           
            }
            now = datetime.now()

            importName = "bridge_" + now.strftime("%Y-%m-%d-%H-%M-%S") + "_" + job_id
            template= self.parameters[f'/{self.env_stage}/edt/api/import_template']
            body = {
                "importName": importName,
                "loadFilePath": imagePath,
                "importTemplate": template
            }  
            try:
                start_time = time.perf_counter()
                api_response = self.http.request('POST', api_url, headers=headers, body=body)
                response_time = time.perf_counter() - start_time
            except (MaxRetryError, NewConnectionError, TimeoutError) as e:
                self.logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"EDT API call failed, Exception: {str(e)}"))
                return None

            if api_response.status < 400:

                self.logger.log_api_call(
                event="DEMS EDT create Import Report API call",
                url=api_url,
                method="POST",
                status_code=api_response.status,
                response_time=response_time,
                job_id=self.job_id
                )

                try:
                    json_data = api_response.json()
                    import_id =  json_data.get("importId")
                
          
                except ValueError as e:
                    self.logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Failed to parse JSON response: {str(e)}"))
                    raise
            elif api_response.status >= 400:
                 self.logger.log_error(event=Constants.PROCESS_NAME, error=Exception(f"Error in EDT API call. API URL : "  +api_url + " Response status : " + str(api_response.status)))
                 return None
            
            if not json_data:
                self.logger.log_error(event=Constants.PROCESS_NAME, error=Exception("No data returned"))
                return

            return import_id

        except Exception as e:
            error_msg = f"EDTDEMS API lookup failed for job_id: {job_id} or importname: {importName}. Error: {str(e)}"
            self.logger.log_error(event="EDTDEMS API lookup Failed", error=error_msg, job_id=self.job_id)
            raise

    def call_dems_api(self, dems_api_url: str, bearer_token: str, agency_code: str,
                      agency_file_number: str) -> tuple:
        """Call DEMS API and return response status and data."""
        headers = {
            'Authorization': f"Bearer {bearer_token}",
            'agencyIdCode': agency_code,
            'agencyFileNumber': agency_file_number
        }
        
        self.logger.log(event="DEMS API Call", status=LogStatus.IN_PROGRESS, message=f"Calling DEMS API with agencyIdCode: {agency_code}", job_id=self.job_id)
        
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

    def update_job_status(self, job_id: str, status_value: str, agency_id_code:str, agency_file_number:str):
        """Update job status in the database."""
        update_job_status = self.db_manager.get_status_code_by_value(value=status_value)
        if update_job_status:
            status_identifier = str(update_job_status["identifier"])
            self.db_manager.update_job_status(
                job_id=job_id,
                status_code=status_identifier,
                job_msg="Called dems edt import requester",
                last_modified_process="lambda: dems import requester"
            )

    def update_evidence_files_import_requested(self, status:str, job_id:str):
        """Update job status in the database."""
        update_job_status = self.db_manager.get_status_code_by_value(value=status)
        file_status_updates_tuple : List[Tuple[str,int]]  # define a list of Tuples<evidence_id, status_code> to update
        if update_job_status:
                status_identifier = str(update_job_status["identifier"])
                evidence_files = self.db_manager.get_evidence_files_by_job(job_id)
                if evidence_files:
                    try:
                        for file in evidence_files:
                            file_status_updates_tuple.append((file["evidence_id"], status_identifier))
                           
                    except KeyError as e:
                        self.logger.log_error(event="Evidence File update Failed", error=str(e), job_id=self.job_id)

                    return_values = self.db_manager.bulk_update_evidence_file_states(file_status_updates_tuple)
                    if return_values['success']:
                        self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Updated evidence files for job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                    else:
                        self.logger.log_error(event="Evidence File update Failed", error=str(e), job_id=self.job_id)

    def send_sqs_message(self, queue_url: str, job_id: str, dems_case_id: str, dems_import_job_id:str,
                         current_timestamp: str,sourcePath:str, custom_exception: Exception = None):
        """Send message to SQS queue."""
        # Define the alphanumeric character pool
        alphanumeric_chars = string.ascii_letters + string.digits

        # Generate a random string of 20 characters
        random_string = ''.join(random.choices(alphanumeric_chars, k=20))

        try:
           
            self.logger.log(event="calling SQS to add msg", status=LogStatus.IN_PROGRESS, message="Trying to call SQS ...")
            
            message_attributes = {
                'Job_id': {'DataType': 'String', 'StringValue': job_id},
                'dems_case_id': {'DataType': 'String', 'StringValue': dems_case_id},
                'dems_import_job_id' : {'DataType': 'String', 'StringValue': dems_import_job_id},
                'sourcePath' : {'DataType': 'String', 'StringValue': sourcePath }
            }
            # Add exception message to message attributes if custom_exception is provided
            if custom_exception:
                message_attributes['ExceptionMessage'] = {
                    'DataType': 'String',
                    'StringValue': str(custom_exception)[:256]  # SQS message attributes have a 256-byte limit
                }
          
            response = self.sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody='Sending SQS message to ' + queue_url,
                DelaySeconds=0,
                MessageGroupId="dems-import-requested",
                MessageDeduplicationId=random_string,
                MessageAttributes=message_attributes
            )
            
            self.logger.log_sqs_message_sent(
                queue_url=queue_url,
                message_id=response,
                response_time_ms=1,
                message_body={
                    "timestamp": current_timestamp,
                    "level": "INFO",
                    "function": Constants.PROCESS_NAME,
                    "event": "SQSMessageQueued",
                    "message": "Queued message for DEMS Import EDT Requester",
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "additional_info": {
                        "target_queue": queue_url,
                        "message_group_id": job_id,
                        "deduplication_id": "file-" + random_string
                    }
                }
            )
        except Exception as e:
            self.logger.log_error(event="SQS Message Send Failed", error=str(e), job_id=self.job_id)

    def process_message(self, message: dict):
        """Process a single SQS message."""
        try:
            self.parse_message_attributes(message)

            job_id = ""
            dems_case_id=""
            destinationPath=""
            import_id=""
            sourcePath=""
            
            receipt_handle = message['receiptHandle']
            messageId = message["messageId"]
            try:
                job_id, sourcePath, dems_case_id,destinationPath = self.parse_message_attributes(message)
                 
            except Exception as e:
                self.logger.log_error(
                event="Message Parsing Failed",
                message=f"Failed to parse message attributes for MessageId: {messageId}",
                error=str(e),
                job_id="unknown",
                message_id=messageId
                )
                return  # Abort processing early

            if not all([job_id, dems_case_id, destinationPath,sourcePath]):
                self.logger.log_error(
                event="Invalid Message Data",
                message="Missing required fields after parsing",
                job_id=job_id,
                message_id=messageId
                )
                return

            import_id: Optional[str] = None
            
            try:
                import_id = self.callEDTDemsApi(job_id, dems_case_id=dems_case_id, imagePath=sourcePath )
            except Exception as mImportIdEx:
                 self.logger.log(
                    event="EDT Dems Import Requester",
                    status=LogStatus.WARNING,
                    message="API called but no import_id returned",
                    job_id=job_id,
                    custom_metadata={"dems_case_id": dems_case_id, "import_file_path": sourcePath}
                    )

            try:
                
                if import_id: 
                    self.update_evidence_files_import_requested(str(Constants.IMPORT_REQUESTED),job_id)
            except Exception as fileUpdateX:
                self.logger.log_error(event="Update Evidence Files", message="Evidence Files Update Failed for Message ID:" + messageId, error=str(fileUpdateX), job_id=self.job_id)
                return

            # Update job status
            try:
                if import_id:
                    self.update_job_status(str(Constants.IMPORT_REQUESTED),job_id)
            except Exception as fileUpdate:
                self.logger.log_error(event="Update Job Status", message="Job Status Update Failed for Message ID:" + messageId, error=str(fileUpdate), job_id=self.job_id)
                return

           # send sqs message 
            try:
                if import_id:
                    queue_url = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import-status']
                    current_timestamp = datetime.now()
                    self.send_sqs_message(queue_url, job_id, dems_case_id=dems_case_id, dems_import_job_id=import_id, sourcePath=sourcePath,
                                           current_timestamp=current_timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            except Exception as sqs_exception:
                self.logger.log_error(event="SQS Message Sending Failed", error=str(msg_err), job_id=self.job_id)
                return
            
            try:
                if import_id:
                    # delete message
                    dems_sqs_url = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import']
                   
                    self.sqs_client.delete_message(QueueUrl = dems_sqs_url, ReceiptHandle=receipt_handle)
                    self.logger.log(
                    event="EDT Dems Import Requester",
                    status=LogStatus.IN_PROGRESS,
                    message="",
                    job_id=job_id,
                    custom_metadata={
                     "dems_case_id": dems_case_id,
                     "import_file_path": sourcePath,
                     "import_id": import_id
                    }
                )
            
            except Exception as e:
                self.logger.log_error(event="SQS Message Delete", message="SQS Message Delete Failed for Message ID:" + messageId, error=str(e), job_id=self.job_id)
                raise
                     
        except Exception as msg_err:
            self.logger.log_error(event="Message Processing Failed", error=str(msg_err), job_id=self.job_id)
            raise


def lambda_handler(event, context):
    """Main Lambda handler function."""
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')
    logger = LambdaStructuredLogger()
    logger.log_start(event="DEMS Import Requester Start", job_id=context.aws_request_id)
    
    try:
        requester = DemsImportRequester(env_stage, logger, context.aws_request_id)

        if not event.get("Records"):
            logger.log(event="SQS Poll", status=Constants.IN_PROGRESS, 
                       message="No messages in queue", job_id=context.aws_request_id)
            return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'No messages to process'}

        for record in event["Records"]:
            attrs = record.get('messageAttributes', {})
            
            # DEBUG: Remove this after confirming it works
            #print("messageAttributes keys:", list(attrs.keys()))

            # Helper to safely extract string value
            def get_value(key):
                attr = attrs.get(key)
                if not attr:
                    return None
                return attr.get('stringValue') or attr.get('binaryValue')

            job_id = get_value('job_id')
            sourcePath = get_value('sourcePath')          # ← FIXED: no space!
            dems_case_id = get_value('dems_case_id')
            destinationPath = get_value('destinationPath') # ← also ensure no typo here

            # Validate all required fields
            missing = []
            if not job_id:          missing.append('job_id')
            if not sourcePath:      missing.append('sourcePath')
            if not dems_case_id:    missing.append('dems_case_id')
            if not destinationPath: missing.append('destinationPath')

            if missing:
                error_msg = f"Missing required message attributes: {', '.join(missing)}"
                logger.log_error(event=Constants.PROCESS_NAME, error=Exception(error_msg), job_id=job_id or "unknown")
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': error_msg,
                        'available_attributes': list(attrs.keys()),
                        'received_event_sample': record  # safe for debugging
                    })
                }

            logger.log_success(
                event=Constants.PROCESS_NAME,
                message=f"Processing job_id: {job_id}, source: {sourcePath}",
                job_id=job_id
            )

            # Pass the full record (or just the values) to your processor
            requester.process_message(record)

        logger.log_success(
            event="Dems Import Requester End",
            message="Successfully completed execution",
            job_id=context.aws_request_id
        )
        return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'Processing complete'}

    except Exception as e:
        logger.log_error(event="Lambda Execution Failed", error=str(e), job_id=getattr(context, 'aws_request_id', 'unknown'))
        raise