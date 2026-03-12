import json
from typing import Dict, List, Optional, Tuple
import boto3
import urllib3
from urllib3.exceptions import MaxRetryError, NewConnectionError, TimeoutError
import os
import time
import botocore.exceptions
import random
import string

from datetime import datetime, timedelta,timezone
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager


class Constants:
    IN_PROGRESS = "IN_PROGRESS"
    ERROR = "ERROR",
    PROCESS_NAME = "transferNotifierCleanup"
    REGION_NAME = "ca-central-1"
    AGENCY_LOOKUP_TABLE_NAME = "agency-lookups"
    NOTIFICATION_MATRIX_TABLE = "notification-distribution"
    HTTP_OK_CODE = 200,
    HTTP_ERROR_CODE = 400,
    IMPORT_REQUESTED= "IMPORT-REQUESTED"
    FIRST_ATTEMPT_COUNT = 1
    CASE_SHARE_RECEIVED_SQS = "q-axon-case-share-received.fifo"
    TRANSFER_COMPLETED_SQS = "q-axon-transfer-completion.fifo"
    NOTIFY_SOURCE_AGENCY = "notify_source_on_complete"
    NOTIFY_BCPS = "notify_bcps_on_complete"
    NOTIFY_SYS_ADMIN = "notify_sysadmin_on_complete"
    VALID_CASE_JOB_STATUS_CODE = "20"
    COMPLETED_STATUS = "100"
    TRANSFER_STATE_NOTIFIER_LAMBDA_NAME = "transfer-state-notifier"
    TRANSFER_EXCEPTION_INITIAL_EXCEP_SQS = "q-transfer-exception-early-notify.fifo"

class TransferNotifierCleanup:
    """Main class to call the EDT create-load-file-import API to initiate "import" of evidence within EDT's S3 bucket to the particular, relevant target DEMS case"""
    def __init__(self, env_stage: str, logger: LambdaStructuredLogger, job_id: str):
            """Initialize the validator with environment stage, logger, and job ID."""
            self.env_stage = env_stage
            self.logger = logger
            self.job_id = job_id
            self.ssm_client, self.sqs_client, self.agency_code_table, self.notification_matrix_table, self.eventbridge = self._initialize_aws_clients()
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
            boto3.resource("dynamodb", region_name=Constants.REGION_NAME).Table(Constants.AGENCY_LOOKUP_TABLE_NAME),
            boto3.resource("dynamodb", region_name=Constants.REGION_NAME).Table(Constants.NOTIFICATION_MATRIX_TABLE),
            boto3.client('events',region_name=Constants.REGION_NAME)
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
            f'/{self.env_stage}/bridge/sqs-queues/lambda-dems-import-requestor-retries',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception-early-notify',
            f'/{self.env_stage}/bridge/notifications/azure_email_tenant',
            f'/{self.env_stage}/bridge/notifications/azure_email_client',
            f'/{self.env_stage}/bridge/notifications/azure_email_secret' ,
            f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-case-share-received',
            f'/{self.env_stage}/bridge/notifications/notify_sysadmin_address',
            f'/{self.env_stage}/bridge/notifications/notify_bcps_address',
            f'/{self.env_stage}/bridge/notifications/notify_prime_address',
            f'/{self.env_stage}/axon/api/categoryId/transferred',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-transfer-completion',
            f'/{self.env_stage}/bridge/s3_retention/transferred',
            f'/{self.env_stage}/bridge/lambda/arn-bridge-s3-cleanup',
            f'/{self.env_stage}/bridge/iam/arn-bridge-s3-cleanup-iam-role'

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
    def determine_email_recipients(self, dynamo_response:any, job_id:str ) -> dict[str,str]:
        """Determine the email recipients based on the provided """

        try:
            email_recipients = {}
            if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            #item.get('notificationAddress')
                            if item.get('SendToBCPS') == 'Y':
                                
                                destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_bcps_address']
                                source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
                                email_recipients = {"destination" : destination, "source_email" : source_email_address}
                                 
                            elif item.get("SendToSysAdmin") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_sysadmin_address']
                                 source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
                                 email_recipients = {"destination" : destination, "source_email" : source_email_address}
                                 
                            elif item.get("SendToAgency") == 'Y':
                                self.sendSourceAgency(job_id,triggerQueue=Constants.TRANSFER_EXCEPTION_QUEUE_NAME)
        except Exception as e:
            return {}

    def sendSourceAgency(self, job_id:str, triggerQueue:str)->bool:
            try:
                source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
                destination_email = None
                return_value = False
                evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
                if evidence_transfer_job:
                    agency_code = evidence_transfer_job["bcpsAgencyIdCode"]
                    dynamo_response = self.agency_code_table.get_item(Key={'bcpsAgencyIdCode': agency_code})
                    if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            destination_email = item.get('notificationAddress')
                    
                if destination_email:
                    return_value = self.sendEmail(job_id=job_id,destination_email=destination_email, source_email=source_email_address, triggerQueue=triggerQueue) 
                    
                return return_value
            except Exception as e:
                self.logger.log_error(
                event="Sending Agency Email Failed",
                error=e,
                job_id=job_id
                )
                return return_value # Abort processing early
            
    def get_access_token(self, tenant_id, client_id, client_secret):
        """ Authenticate with Azure AD and get OAuth token for Microsoft Graph API
    
        Args:
        tenant_id: Azure AD Tenant ID
        client_id: Azure AD Application (Client) ID
        client_secret: Azure AD Client Secret (decrypted from SecureString)
        """
        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    
        data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,  # Using decrypted secret from SSM
        'scope': 'https://graph.microsoft.com/.default'
        }
    
        response = self.http.post(url, data=data, timeout=10)
        response.raise_for_status()
    
        return response.json()['access_token']   
    def sendEmail(self, job_id:str, destination_emails:list[str], source_email:str, triggerQueue:str, subject_line:str, body_text:str, access_token:str)->bool:
        """
        Send email via Microsoft Graph API
        Args:
            token: OAuth access token
            from_address: Sender email address (must be authorized)
            to_addresses: List of recipient email addresses
            subject: Email subject line
            body: Email body content (plain text)
        """
       # Create the email message
        url = f"https://graph.microsoft.com/v1.0/users/{source_email}/sendMail"

        headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
        }
        try:
            self.logger.log_start("start send email", job_id=job_id) 
             # Build recipient list
            to_recipients = [
                {'emailAddress': {'address': email}} 
                for email in destination_emails
            ]
            email_message = {
            'message': {
            'subject': subject_line,
            'body': {
                'contentType': 'Text',
                'content': body_text
                },
                'toRecipients': to_recipients
                },
            'internetMessageHeaders': [
                {  
                    'name': 'x-ms-exchange-organization-encryptmessage',
                    'value': 'true'
                }
            ]
            }
            self.logger.log_success(event="Sending email succeeded.", job_id=job_id)
            return True
        except Exception as e:
            self.logger.log_error(
                event="Sending email failed.",
                
                error=e,
                job_id=job_id,
               
                )
            return False # Abort processing early
        
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

        job_id = get_attr('job_id') or get_attr('JobId') or get_attr('jobid')
        sourcePath = get_attr('sourcePath') or get_attr('SourcePath') or get_attr('source_path')
        dems_case_id = get_attr('dems_case_id') or get_attr('demscaseid') or get_attr('DemsCaseId')
        destinationPath = get_attr('destination_path') or get_attr('DestinationPath')
        first_attempt_time = get_attr('first_attempt_time') or get_attr('FirstAttemptTime')
        last_attempt_time = get_attr('last_attempt_time') or get_attr('LastAttemptTime')
        attempt_number = get_attr('attempt_number') or get_attr('AttemptNumber')

        required = {
            "job_id": job_id,
            "sourcePath": sourcePath,
            "dems_case_id": dems_case_id,
            "destinationPath": destinationPath,
            "first_attempt_time" : first_attempt_time,
            "last_attempt_time"  : last_attempt_time,
            "attempt_number"     : attempt_number
        }

        missing = [k for k, v in required.items() if not v]
        if missing:
            
            raise ValueError(f"Missing required field(s): {', '.join(missing)}")

        attempt_number_int = int(attempt_number)
        #job_id, sourcePath, dems_case_id,destinationPath, first_attempt_time, last_attempt_time, attempt_number
        return job_id, sourcePath, dems_case_id, destinationPath, first_attempt_time, last_attempt_time,attempt_number_int

    def lambda_update_job_status( self, job_id: str, status_value: str, msg:str):
        """Update job status in the database."""
        job_status_code = self.db_manager.get_status_code_by_value(value=status_value)
        if job_status_code:
            status_identifier = str(job_status_code["identifier"])
            self.db_manager.update_job_status(
                job_id=job_id,
                status_code=status_identifier,
                job_msg=msg,
                last_modified_process=Constants.PROCESS_NAME
            )
       
    def process_exception_message(self, job_id:str, message_handle:str, source_case_title:str, job_msg:str, source_queue_url:str )->bool:
        status_code = "IMPORT_FAILED"

        self.update_job_status(job_id,status_code, None, None,job_msg)
        current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
        self.logger.log(
                    function="axonRccAndDemsCaseValidator",
                    event=Constants.PROCESS_NAME,
                    status=Constants.ERROR,
                    message=job_msg,
                    job_id=job_id,
                    custom_metadata=[{"job_id" : job_id}]
                )
    
        self.sqs_client.delete_message(
                    QueueUrl=source_queue_url,
                    ReceiptHandle=message_handle
                )
        self.logger.info(f"Deleted message: {message_handle}")
        message_attributes = {
            "job_id"                        : job_id,
            "source_case_title"             : source_case_title,
            "exception_agency_match"        : current_timestamp

         }
        self.send_sqs_message('q-transfer-exception.fifo', job_id, None, current_timestamp, Exception("Download attempt max exceeded"), None, message_attributes=message_attributes)

    
    def send_event_to_event_bridge(self, job_id:str)-> dict:
        try:
                retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferred']
                int_retention_days = int(retention_days)
                cleanup_date = datetime.utcnow() + timedelta(days=int_retention_days)
                schedule_expression = f'at({cleanup_date.strftime("%Y-%m-%dT%H:%M:%S")})'
                RoleArn = self.parameters[f'/{self.env_stage}/bridge/iam/arn-bridge-s3-cleanup-iam-role']
                transfer_job = self.db_manager.get_transfer_job(job_id)
                if transfer_job:
                    source_path = transfer_job['source_case_id'] + " _ " + transfer_job['dems_case_id'] + "_" + job_id                    
                destination_path = transfer_job['destination_path']
                # Define the event
                notification_body = {
                     "job_id" : job_id,
                     "cleanup_date" : cleanup_date ,
                     "schedule_expression" : schedule_expression,
                     "source_path" : source_path,
                     "destination_path" : destination_path
                } 
               
                entries = [
                    {
                    'Source': Constants.TRANSFER_STATE_NOTIFIER_LAMBDA_NAME,  # Required: Identify the source
                    'DetailType': 'MyCustomEvent',    # Required: Event category
                    'Detail': json.dumps(notification_body),
                    'EventBusName': 'default'        # Optional: Use a custom bus if needed
                    }
                ]       
    
                # Put the event to EventBridge
                rule_name = "call_to_bridge_s3_cleaner"

                target_arn = self.parameters[f'/{self.env_stage}/bridge/lambda/arn-bridge-s3-cleanup']  
                response = self.eventbridge.put_events(
                     Name=rule_name, 
                     schedule_expression= schedule_expression,
                     Description='Scheduled EventBridge Rule for S3 Cleaner', 
                     Targets=[
                          {
                               'Id' : job_id,
                               'Arn': target_arn

                          }
                     ],
                     RoleArn=RoleArn,
                     Entries=entries)
    
                # Check the response for any failed entries
                if response['FailedEntryCount'] > 0:
                     raise Exception("Event publishing failed")
        
                
        except Exception as msg_err:
                self.logger.log_info(event="Sending Processing Failed", job_id=self.job_id)
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise

           
    def process_case_share_received(self,  message:dict)-> bool:
        receipt_handle = message['receiptHandle']
        messageId = message["messageId"]
        source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
        returnEmail = False
        try:
            
            evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
            attrs = message.get('messageAttributes', {})
            destinationEmails = []
            job_id = self.get_attr('job_id',attrs)

            if not job_id:
                self.logger.log_error(
                event="Retrieving Job Id failed",
               
                error=None,
                job_id="unknown",
               
                )
                return  # Abort processing early
            # get notification values by job code
            self.logger.log_start(event="Processing case share received sqs", job_id=job_id)
            dynamo_response = self.notification_matrix.get_item(Key={'JobStatusCodeId': Constants.VALID_CASE_JOB_STATUS_CODE})
            if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            #item.get('notificationAddress')
                            body_text = item.get('Body')
                            subject = item.get('Subject')
                            if body_text:
                               
                                 if evidence_transfer_job:
                                     body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                     body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                     body_text = body_text.replace('{{dems-case-id}}', evidence_transfer_job['dems_case_id'])
                                     body_text = body_text.replace('{{agency-id-code}}', evidence_transfer_job['agency_id_code'])
                                     body_text = body_text.replace('{{agency-file-id}}', evidence_transfer_job['agency_file_number'])
                                     agency_code = evidence_transfer_job["bcpsAgencyIdCode"]

                            tenant_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_tenant']
                            client_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_client']
                            client_secret = self.parameters[ f'/{self.env_stage}/bridge/notifications/azure_email_secret']

                            access_token = self.get_access_token(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

                            if item.get('SendToBCPS') == 'Y':
                                    destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_bcps_address']
                                    destinationEmails.append(destination)

                            elif item.get("SendToSysAdmin") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_sysadmin_address']
                                 destinationEmails.append(destination)
                            elif item.get("SendToAgency") == 'Y':
                               if agency_code:
                                    agency_dynamo_response = self.agency_code_table.get_item(Key={'bcpsAgencyIdCode': agency_code})
                                    if agency_dynamo_response:
                                         item = dynamo_response.get('Item')
                                         if item :
                                            destination = item.get('notificationAddress')
                                            destinationEmails.append(destination)
                                            
                            elif item.get("SendToPRIME") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_prime_address']
                                 destinationEmails.append(destination)
                                
            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.CASE_SHARE_RECEIVED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
            if returnEmail:
                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-case-share-received'], ReceiptHandle=receipt_handle)
                
                return True
            else:
                self.logger.log_error(
                    event="Message Sending Failed",
                    error=Exception("Error sending email "),
                    job_id=job_id
                    )
                return False
                               
        except Exception as msg_err:
            self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
            raise

    def determineReceipientEmails (self, item, agency_code:str) -> List[str]:
        """Determine the destination emails based on the notification matrix value """
        destinationEmails = []
        if item.get('SendToBCPS') == 'Y':
                destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_bcps_address']
                destinationEmails.append(destination)

        elif item.get("SendToSysAdmin") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_sysadmin_address']
                                 destinationEmails.append(destination)
        elif item.get("SendToAgency") == 'Y':
                               if agency_code:
                                    agency_dynamo_response = self.agency_code_table.get_item(Key={'bcpsAgencyIdCode': agency_code})
                                    if agency_dynamo_response:
                                         item = agency_dynamo_response.get('Item')
                                         if item :
                                            destination = item.get('notificationAddress')
                                            destinationEmails.append(destination)
                                            
        elif item.get("SendToPRIME") == 'Y':
                destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_prime_address']
                destinationEmails.append(destination)
        return destinationEmails
    
    def process_transfer_completed(self, message:dict)-> bool:
        """When a transfer completes in its entirety, successfully.  Distribute a notice (if configured) accordingly.
        """
        receipt_handle = message['receiptHandle']
        messageId = message["messageId"]
        source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
        returnEmail = False
        try:
            job_id = self.get_attr('job_id',attrs)
            evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
            attrs = message.get('messageAttributes', {})
            destinationEmails = []
            job_msg = evidence_transfer_job['job_msg']

            if not job_id:
                self.logger.log_error(
                event="Retrieving Job Id failed",
               
                error=None,
                job_id="unknown",
               
                )
                return  # Abort processing early
            # get notification values by job code
            self.logger.log_start(event="Processing transfer completion sqs", job_id=job_id)
            dynamo_response = self.notification_matrix.get_item(Key={'JobStatusCodeId': Constants.VALID_CASE_JOB_STATUS_CODE})
            if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            #item.get('notificationAddress')
                            body_text = item.get('Body')
                            subject = item.get('Subject')
                            if body_text:
                               
                                 if evidence_transfer_job:
                                     body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                     body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                     body_text = body_text.replace('{{dems-case-id}}', evidence_transfer_job['dems_case_id'])
                                     body_text = body_text.replace('{{agency-id-code}}', evidence_transfer_job['agency_id_code'])
                                     body_text = body_text.replace('{{agency-file-id}}', evidence_transfer_job['agency_file_number'])
                                     body_text = body_text.replace('{{evidence-received}}', evidence_transfer_job['source_case_evidence_count_to_download'])
                                     evidence_files = self.db_manager.get_evidence_files_by_job(job_id)
                                     transferred_count = 0
                                     if evidence_files:
                                          filtered_files = [file for file in evidence_files if file['evidence_transfer_state_code'] == '100']
                                          if filtered_files:
                                            transferred_count = filtered_files.count()
                                     body_text = body_text.replace('{{evidence-transferred}}', str(transferred_count))
                                     agency_code = evidence_transfer_job["bcpsAgencyIdCode"]

                            tenant_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_tenant']
                            client_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_client']
                            client_secret = self.parameters[ f'/{self.env_stage}/bridge/notifications/azure_email_secret']

                            access_token = self.get_access_token(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
                            destinationEmails = self.determineReceipientEmails(item,agency_code)

                            """"
                            if item.get('SendToBCPS') == 'Y':
                                    destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_bcps_address']
                                    destinationEmails.append(destination)

                            elif item.get("SendToSysAdmin") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_sysadmin_address']
                                 destinationEmails.append(destination)
                            elif item.get("SendToAgency") == 'Y':
                               if agency_code:
                                    agency_dynamo_response = self.agency_code_table.get_item(Key={'bcpsAgencyIdCode': agency_code})
                                    if agency_dynamo_response:
                                         item = dynamo_response.get('Item')
                                         if item :
                                            destination = item.get('notificationAddress')
                                            destinationEmails.append(destination)
                                            
                            elif item.get("SendToPRIME") == 'Y':
                                 destination = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_prime_address']
                                 destinationEmails.append(destination)
                            """    
            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.TRANSFER_COMPLETED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
            if returnEmail:
                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-transfer-completion'], ReceiptHandle=receipt_handle)
                sqs_body = {
                       "job_id": job_id,
                       "axon_category_id" : self.parameters[f'/{self.env_stage}/axon/api/categoryId/transferred']

                }
                messageGroupId = f'job-id"{job_id}-transfer-completed-{evidence_transfer_job['agency_file_number']}'
                messageDeDupId = f'job-id"{job_id}-unique-id"{evidence_transfer_job['agency_file_number']}'
                
                sqs_return = self.create_sqs_message(job_id=job_id, queue_url=self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-evidence-category-update'],body_json=sqs_body, messageGroupId=messageGroupId, messageDeDupId=messageDeDupId)
                if not sqs_return:
                     self.logger.log_error(event="Transfer Process Notifier failed to create SQS message", error= Exception("Transfer Process Notifier failed to create SQS message"))
                     return {'statusCode': Constants.HTTP_ERROR_CODE, 'body': 'Processing complete'} 
                
                eventBridgeReturn = self.send_event_to_event_bridge(job_id)
                job_msg =  job_msg + " transfer state notification sent. axon category update initiated. bridge s3 cleanup scheduled"
                self.lambda_update_job_status(job_id, Constants.COMPLETED_STATUS,job_msg )
                evidence_files = self.db_manager.get_evidence_files_by_job(job_id)
        
                if not evidence_files:
                    self.logger.log(
                    event=Constants.PROCESS_NAME,
                    level=LogLevel.WARNING,
                    status=LogStatus.SUCCESS,
                    message="axon_evidence_data_transferor_no_evidence_files_found",
                    context_data={
                        "env_stage": self.env_stage,
                        "job_id": job_id,
                    })
                    return {
                    'success': True,
                    'job_updated': True,
                    'files_updated': 0
                    }
                else:
                    self.logger.log(
                    event=Constants.PROCESS_NAME,
                    level=LogLevel.INFO,
                    status=LogStatus.SUCCESS,
                    message="axon_evidence_data_transferor_evidence_files_found",
                    context_data={
                    "env_stage": self.env_stage,
                    "job_id": job_id,
                    "file_count": len(evidence_files)
                    }
                    )
                    
                job_status_code = self.db_manager.get_status_code_by_value(value=Constants.COMPLETED_STATUS)
                if job_status_code:
                    status_identifier = str(job_status_code["identifier"])
                    evidence_file_updates = "UPDATE EVIDENCE_FILES SET EVIDENCE_TRANSFER_STATE=%s,bridge_s3_cleanup_scheduled_date=%s,bridge_s3_cleanup_scheduled=1,last_modified_process=%s, last_modified_utc=NOW() where evidence_id=%s"
                    for evidence_file in evidence_files:
                        evidence_id = evidence_file.get('evidence_id')
                        self.db_manager.execute_query_one(
                            query=evidence_file_updates,
                            params=(
                                status_identifier,
                                datetime.now().strftime("%Y-%m-%d %H:%M"),
                                "lambda: transfer process notifier",
                                evidence_id
                            ), autoCommit=True)

                return True
            else:
                self.logger.log_error(
                    event="Message Sending Failed",
                    error=Exception("Error sending email "),
                    job_id=job_id
                    )
                return False
                               
        except Exception as msg_err:
            self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
            raise

    def notify_first_exception_failure(self, job_id:str,message:dict)-> bool:
           """When a transfer encounters an exception of a specific type that can be retried thereafter (such as when a DEMS case is not yet created), distribute an initial notice"""
           attrs = message.get('messageAttributes', {})
           JobStatusCodeId  = attrs['JobStatusCodeId']['stringValue']
           self.logger.log_start(event="Processing transfer first exception sqs", job_id=job_id)
           dynamo_response = self.notification_matrix.get_item(Key={'JobStatusCodeId': JobStatusCodeId})
           receipt_handle = message['receiptHandle']
           messageId = message["messageId"]
           source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
           try:
                job_id = self.get_attr('job_id',attrs)
                evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
                attrs = message.get('messageAttributes', {})
                destinationEmails = []
                job_msg = evidence_transfer_job['job_msg']
                
                if not job_id:
                    self.logger.log_error(
                    event="Retrieving Job Id failed",
               
                    error=None,
                    job_id="unknown",
               
                    )
                    return  # Abort processing early
                
                if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            #item.get('notificationAddress')
                            body_text = item.get('Body')
                            subject = item.get('Subject')
                            if body_text:
                               
                                 if evidence_transfer_job:
                                     body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                     body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                   
                                     agency_code = evidence_transfer_job["bcpsAgencyIdCode"]

                            tenant_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_tenant']
                            client_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_client']
                            client_secret = self.parameters[ f'/{self.env_stage}/bridge/notifications/azure_email_secret']

                            access_token = self.get_access_token(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
                            destinationEmails = self.determineReceipientEmails(item,agency_code)
                            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.TRANSFER_COMPLETED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
                            if returnEmail:
                                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception'], ReceiptHandle=receipt_handle)
                        return True
           except Exception as msg_err:
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise
            
                     


    def create_sqs_message(self, job_id: str, queue_url:str, source_case_id: str, body_json:str, messageGroupId:str, messageDeDupId:str) -> Dict:
        """Create a properly formatted SQS message for evidence download."""
        try:

            self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(body_json),
                MessageGroupId=messageGroupId,
                MessageDeduplicationId=messageDeDupId
            
                )
            self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent exception message to queue for job {job_id}")
            return True
        except Exception as e:
             
            self.logger.log_error(event=Constants.PROCESS_NAME, error=e)
            return False
    def process_transfer_exception_sqs(self, message:dict)-> bool:
           """Process a transfer exception message from SQS."""
            
           attrs = message.get('messageAttributes', {})
           JobStatusCodeId  = attrs['JobStatusCodeId']['stringValue']
           job_id = self.get_attr('job_id',attrs)
           self.logger.log_start(event="Processing transfer first exception sqs", job_id=job_id)
           
           receipt_handle = message['receiptHandle']
           messageId = message["messageId"]
           source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
           try:
                job_id = self.get_attr('job_id',attrs)
                evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
                attrs = message.get('messageAttributes', {})
                destinationEmails = []
                job_msg = evidence_transfer_job['job_msg']
                job_status_code = ""
                if evidence_transfer_job:
                     job_status_code = evidence_transfer_job['job_status_code']

                
                if not job_id:
                    self.logger.log_error(
                    event="Retrieving Job Id failed",
               
                    error=None,
                    job_id="unknown",
               
                    )
                    return  # Abort processing early
                
                if job_status_code:
                        
                        current_timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')
                        dynamo_response = self.notification_matrix.get_item(Key={'JobStatusCodeId': job_status_code})
                        item = dynamo_response.get('Item')
                        if item :
                            #item.get('notificationAddress')
                            body_text = item.get('Body')
                            subject = item.get('Subject')
                            if body_text:
                               
                                 if evidence_transfer_job:
                                     agency_code = evidence_transfer_job["bcpsAgencyIdCode"]
                                     if job_status_code == '21': # invalid-case
                                     
                                      body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                      body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                      body_text = body_text.replace('{{current-utc}}', current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
                                     elif job_status_code == '22': #INVALID-AGENCY-IDENTIFIER
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                        body_text = body_text.replace('{{current-utc}}', current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
                                     elif job_status_code == '110' : #FAILED
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                     elif job_status_code.contains('83','84')  : #IMPORTED WITH ERRORS
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                    

                            tenant_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_tenant']
                            client_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_client']
                            client_secret = self.parameters[ f'/{self.env_stage}/bridge/notifications/azure_email_secret']

                            access_token = self.get_access_token(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
                            destinationEmails = self.determineReceipientEmails(item,agency_code)
                            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.TRANSFER_COMPLETED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
                            if returnEmail:
                                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception'], ReceiptHandle=receipt_handle)
                        return True
           except Exception as msg_err:
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise
          

    def lambda_handler(event, context):
        """Main Lambda handler function."""
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        logger = LambdaStructuredLogger()
        logger.log_start(event="DEMS Import Requester Start", job_id=context.aws_request_id)
    
        try:
            notifier = TransferNotifierCleanup(env_stage, logger, context.aws_request_id)

            if not event.get("Records"):
                logger.log(event="SQS Poll", status=Constants.IN_PROGRESS, 
                       message="No messages in queue", job_id=context.aws_request_id)
                return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'No messages to process'}

            for record in event["Records"]:
                attrs = record.get('messageAttributes', {})
                queue_arn = record['eventSourceARN']
                queue_name = notifier.get_sqs_queue_calling(queue_arn)
                job_id = ""
            
                if queue_name == Constants.CASE_SHARE_RECEIVED_SQS:
                    notifier.process_case_share_received(record)
                
                elif queue_name == Constants.TRANSFER_COMPLETED_SQS:
                    transfer_return = notifier.process_transfer_completed(record)
                    if not transfer_return:
                         logger.log_error(event=Constants.PROCESS_NAME,error=Exception("Commpleted event bridge not set"), job_id=job_id)
                    else:
                        job_id = transfer_return.get('job_id')
                elif queue_name == Constants.TRANSFER_EXCEPTION_INITIAL_EXCEP_SQS:
                     notifier_return = notifier.notify_first_exception_failure(job_id)
               

                # Pass the full record (or just the values) to your processor
                #notifier.process_message(record)

            logger.log_success(
                event="Transfer Notifier End",
                message="Successfully completed execution",
                job_id=context.aws_request_id
            )
            return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'Processing complete'}

        except Exception as e:
            logger.log_error(event="Lambda Execution Failed", error=str(e), job_id=getattr(context, 'aws_request_id', 'unknown'))
            raise