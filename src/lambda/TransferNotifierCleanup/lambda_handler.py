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
    HTTP_OK_CODE = 200
    HTTP_ERROR_CODE = 400
    IMPORT_REQUESTED= "IMPORT-REQUESTED"
    FIRST_ATTEMPT_COUNT = 1
    CASE_SHARE_RECEIVED_SQS = "q-axon-case-share-received.fifo"
    TRANSFER_COMPLETED_SQS = "q-axon-transfer-completion.fifo"
    TRANSFER_EXCEPTION_SQS = "q-transfer-exception.fifo"
    NOTIFY_SOURCE_AGENCY = "notify_source_on_complete"
    NOTIFY_BCPS = "notify_bcps_on_complete"
    NOTIFY_SYS_ADMIN = "notify_sysadmin_on_complete"
    VALID_CASE_JOB_STATUS_CODE = "20"
    COMPLETED_STATUS = "100"
    TRANSFER_STATE_NOTIFIER_LAMBDA_NAME = "transfer-state-notifier"
    TRANSFER_EXCEPTION_INITIAL_EXCEP_SQS = "q-transfer-exception-early-notify.fifo"
    INACTIVE_CASE_STATUS = "21"
    INVALID_AGENCY_IDENTIFIER = "22"
    FAILED = "110"
    IMPORT_FAILED = "83"
    IMPORTED_WITH_ERRORS = "84"
    IMPORTED = "82"
    REJECTED_CATEGORY = "rejected"
    FAILED_CATEGORY = "failed"
    TRANSFER_ISSUES = "transferredIssues"

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
            f'/{self.env_stage}/bridge/iam/arn-bridge-eventbridgeschedulerexecutionrole',
            f'/{self.env_stage}/axon/api/categoryId/failed',
            f'/{self.env_stage}/bridge/s3_retention/failed',
            f'/{self.env_stage}/axon/api/categoryId/rejected',
            f'/{self.env_stage}/bridge/s3_retention/rejected',
            f'/{self.env_stage}/axon/api/categoryId/transferredIssues',
            f'/{self.env_stage}/bridge/s3_retention/transferredIssues',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-evidence-category-update'
            ]
        
        try:
            #ssm_response = self.ssm_client.get_parameters(Names=parameter_names, WithDecryption=True)
           #parameters = {param['Name']: param['Value'] for param in ssm_response['Parameters']}
            parameters = {}
            batch_size = 10
            for i in range(0, len(parameter_names), batch_size):
                batch = parameter_names[i:i + batch_size]
                ssm_response = self.ssm_client.get_parameters(Names=batch, WithDecryption=True)
    
                if 'InvalidParameters' in ssm_response and len(ssm_response['InvalidParameters']) > 0:
                    raise ValueError(f"Invalid parameters: {ssm_response['InvalidParameters']}")
    
                for param in ssm_response['Parameters']:
                    parameters[param['Name']] = param['Value']

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

    
    def send_event_to_event_bridge(self, job_id:str, retention_days:str)-> dict:
        try:
                if retention_days is None:
                    retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferred']

                int_retention_days = int(retention_days)
                cleanup_date = datetime.utcnow() + timedelta(days=int_retention_days)
                schedule_expression = f'at({cleanup_date.strftime("%Y-%m-%dT%H:%M:%S")})'
                RoleArn = self.parameters[f'/{self.env_stage}/bridge/iam/arn-bridge-eventbridgeschedulerexecutionrole']
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
                    'DetailType': 'S3Cleanup_Scheduled',    # Required: Event category
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
        
                self.logger.log_success(event="EventBridge schedule created successfully", message=" Schedule ARN : " + response['Entries'][0]['EventId'] + ". S3 Paths to cleanup : " + source_path + "/ and " + source_path + ".zip" )
                return {
                     "job_id" : job_id,
                     "cleanup_date" : cleanup_date ,
                     "schedule_expression" : schedule_expression,
                     "schedule_arn" : target_arn,
                     "schedule_name" : rule_name,
                     "retention_days" : retention_days

                }
        except Exception as msg_err:
                self.logger.log_info(event="Sending Processing Failed", job_id=self.job_id)
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise

    def prepare_final_logging(self, job_id:str, status_code:str, source_case_id:str, evidence_file_len:int, retention_days:int, cleanup_sched_date:str,sqs_msg_id:str,sched_arn:str, sched_name:str, outcome:str) -> dict:
       return_dict = {
            "status_code" : status_code,
            "body" :{
                 "job_id": job_id,
                 "source_case_id" : source_case_id,
                 "outcome" : outcome,
                 "base_name" : Constants.PROCESS_NAME,
                 "evidence_files" : evidence_file_len,
                 "retention_days" : retention_days,
                 "cleanup_scheduled_date " : cleanup_sched_date,
                 "axon_cleanup" : {
                      "status" : "queued",
                      "sqs_message_id" : sqs_msg_id
                 },
                 "s3_cleanup" : {
                      "status" : "scheduled",
                      "schedule_arn" : sched_arn,
                      "schedule_name" : sched_name
                 }
            }
       }
       return return_dict

    def process_case_share_received(self,  message:dict)-> dict:
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
                evidence_files = self.db_manager.get_evidence_files(job_id)
                return self.prepare_final_logging(job_id, Constants.HTTP_OK_CODE, evidence_transfer_job['source_case_id'], len(evidence_files), None, None,None,None, "Process Case Share Processed" )
            else:
                self.logger.log_error(
                    event="Message Sending Failed",
                    error=Exception("Error sending email "),
                    job_id=job_id
                    )
                return {
                       "job_id": job_id,

                }
                               
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
    
    def process_transfer_completed(self, message:dict)-> dict:
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

                            
            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.TRANSFER_COMPLETED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
            if returnEmail:
                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-transfer-completion'], ReceiptHandle=receipt_handle)
                sqs_body = {
                       "job_id": job_id,
                       "axon_category_id" : self.parameters[f'/{self.env_stage}/axon/api/categoryId/transferred']

                }
                agency_file_number = evidence_transfer_job['agency_file_number']
                messageGroupId = f'job-id-{job_id}-transfer-completed-{agency_file_number}'
                messageDeDupId = f'job-id-{job_id}-unique-id-{agency_file_number}'
                
                sqs_return = self.create_sqs_message(job_id=job_id, queue_url=self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-axon-evidence-category-update'],body_json=sqs_body, messageGroupId=messageGroupId, messageDeDupId=messageDeDupId)
                if not sqs_return:
                     self.logger.log_error(event="Transfer Process Notifier failed to create SQS message", error= Exception("Transfer Process Notifier failed to create SQS message"))
                     return {'statusCode': Constants.HTTP_ERROR_CODE, 'body': 'Processing complete'} 
                
                eventBridgeReturn = self.send_event_to_event_bridge(job_id, None)
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
                                eventBridgeReturn['cleanup_date'],
                                "lambda: transfer process notifier",
                                evidence_id
                            ), autoCommit=True)

                return self.prepare_final_logging(job_id, Constants.HTTP_OK_CODE, evidence_transfer_job['source_case_id'] , len(evidence_files),int(retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferred']),
                                                   eventBridgeReturn['cleanup_date'],sqs_return['sqs_message_id'],eventBridgeReturn['schedule_arn'],eventBridgeReturn['schedule_name'], "Process Transfer Completed successful")
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

    def notify_first_exception_failure(self, job_id:str,message:dict)-> dict:
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
                                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception-early-notify'], ReceiptHandle=receipt_handle)
                        return self.prepare_final_logging(job_id, Constants.HTTP_OK_CODE, evidence_transfer_job['source_case_id'] , 0,int(retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferred']),
                                                   None,None,None,None, "Process Transfer Completed successful")
           except Exception as msg_err:
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise
            
                     


    def create_sqs_message(self, job_id: str, queue_url:str, source_case_id: str, body_json:str, messageGroupId:str, messageDeDupId:str) -> Dict:
        """Create a properly formatted SQS message for evidence download."""
        try:

           response =  self.sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(body_json),
                MessageGroupId=messageGroupId,
                MessageDeduplicationId=messageDeDupId
            
            )
           self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent exception message to queue for job {job_id}")
           return {
                 "sqs_message_id" : response['MessageId']
                }
        except Exception as e:
             
            self.logger.log_error(event=Constants.PROCESS_NAME, error=e)
            return False
    def process_transfer_exception_sqs(self, message:dict)-> dict:
           """Process a transfer exception message from SQS."""
            
           attrs = message.get('messageAttributes', {})
           JobStatusCodeId  = attrs['JobStatusCodeId']['stringValue']
           job_id = self.get_attr('job_id',attrs)
           self.logger.log_start(event="Processing transfer first exception sqs", job_id=job_id)
           
           receipt_handle = message['receiptHandle']
           messageId = message["messageId"]
           source_email_address = self.parameters[f'/{self.env_stage}/bridge/notifications/from_address']
           evidence_files_len = 0
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
                                     if job_status_code == Constants.INACTIVE_CASE_STATUS: # invalid-case
                                     
                                      body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                      body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                      body_text = body_text.replace('{{current-utc}}', current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
                                     elif job_status_code == Constants.INVALID_AGENCY_IDENTIFIER: #INVALID-AGENCY-IDENTIFIER
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                        body_text = body_text.replace('{{current-utc}}', current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
                                     elif job_status_code == Constants.FAILED : #FAILED
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                     elif job_status_code.contains(Constants.IMPORT_FAILED, Constants.IMPORTED_WITH_ERRORS)  :
                                        body_text = body_text.replace('{{source-case-title}}', evidence_transfer_job['source_case_title'])
                                        body_text = body_text.replace('{{shared-on}}', evidence_transfer_job['source_case_last_modified_utc'])
                                        body_text = body_text.replace('{{current-utc}}', current_timestamp.strftime("%Y-%m-%dT%H:%M:%S"))
                                        body_text = body_text.replace('{{dems-case-id}}', evidence_transfer_job['dems_case_id'])
                                        body_text = body_text.replace('{{agency-id-code}}', evidence_transfer_job['agency_id_code'])
                                        body_text = body_text.replace('{{agency-file-id}}', evidence_transfer_job['agency_file_number'])
                                        # process associated evidence files
                                        evidence_files = self.db_manager.get_evidence_files_by_job(job_id)
                                        evidence_files_len = len(evidence_files)
                                        if evidence_files:
                                        # Define status categories and their corresponding placeholders
                                            status_configs = [
                                            {
                                            'status': Constants.IMPORTED,
                                            'placeholder': '{successful-imports}',
                                            'format': lambda file: file['evidence_file_name']
                                            },
                                            {
                                            'status': Constants.IMPORTED_WITH_ERRORS,
                                            'placeholder': '{files-imported-with-warnings}',
                                            'format': lambda file: f"{file['evidence_file_name']}: {file.get('dems_imported_error_msg', '')}"
                                            },
                                            {
                                            'status': Constants.IMPORT_FAILED,
                                            'placeholder': '{files-failed-during-import}',
                                            'format': lambda file: f"{file['evidence_file_name']}: {file.get('dems_imported_error_msg', '')}"
                                            }
                                        ]           

                                        for config in status_configs:
                                            filtered_files = [file for file in evidence_files if file['evidence_transfer_state_code'] == config['status']]
                                            if filtered_files:
                                                lines = [config['format'](file) for file in filtered_files]
                                                content = '\n'.join(lines)
                                                if content:
                                                    body_text = body_text.replace(config['placeholder'], content)

                            tenant_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_tenant']
                            client_id = self.parameters[f'/{self.env_stage}/bridge/notifications/azure_email_client']
                            client_secret = self.parameters[ f'/{self.env_stage}/bridge/notifications/azure_email_secret']

                            access_token = self.get_access_token(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
                            destinationEmails = self.determineReceipientEmails(item,agency_code)
                            returnEmail = self.sendEmail(job_id, source_email=source_email_address, destination_emails=destinationEmails,  triggerQueue=Constants.TRANSFER_COMPLETED_SQS, subject_line=subject, body_text=body_text, access_token=access_token)
                            if returnEmail:
                                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                                self.sqs_client.delete_message(QueueUrl = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception'], ReceiptHandle=receipt_handle)
                            
                            # Determine category type
                            category_type = self.determine_category_type(job_status_code)
                            axon_category_id = ""
                            retention_days = ""
                            if category_type == Constants.REJECTED_CATEGORY:
                                 axon_category_id = self.parameters[f'/{self.env_stage}/axon/api/categoryId/rejected']
                                 retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/rejected']
                            elif category_type == Constants.FAILED_CATEGORY:
                                axon_category_id = self.parameters[f'/{self.env_stage}/axon/api/categoryId/failed']
                                retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/failed']
                            elif category_type == Constants.TRANSFER_ISSUES:
                                axon_category_id = self.parameters[f'/{self.env_stage}/axon/api/categoryId/transferredIssues']
                                retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferredIssues']

                            sqs_json = {
                                 "job_id" : job_id,
                                 "axon_category_id" : axon_category_id
                            }
                            sqs_return = self.create_sqs_message ( job_id, self.parameters[f'/{self.env_stage}/q-axon-evidence-category-update.fifo'],"",sqs_json,f'axon-bridge-job-{job_id}', f'axon-bridge-catid-{axon_category_id}')
                            event_bridge_return = self.send_event_to_event_bridge(job_id, retention_days)
                            if sqs_return:
                                self.logger.log_sqs_message_sent(self.parameters[f'/{self.env_stage}/q-axon-evidence-category-update.fifo'],sqs_return['sqs_message_id'],sqs_json, 10,job_id=job_id)
                        return self.prepare_final_logging(job_id, Constants.HTTP_OK_CODE, evidence_transfer_job['source_case_id'] , evidence_files_len,int(retention_days = self.parameters[f'/{self.env_stage}/bridge/s3_retention/transferred']),
                                                   event_bridge_return['cleanup_date'],event_bridge_return['schedule_arn'],event_bridge_return['schedule_name'],'Process Transfer Exception Completed.', "Process Transfer Completed successful")
           except Exception as msg_err:
                self.logger.log_error(event="Sending Processing Failed", error=str(msg_err), job_id=self.job_id)
                raise
          
    def determine_category_type(self, input_job_status:str)-> str:
       if input_job_status:
            if input_job_status == Constants.INACTIVE_CASE_STATUS:
                 return Constants.REJECTED_CATEGORY
            elif input_job_status == Constants.INVALID_AGENCY_IDENTIFIER:
                 return Constants.REJECTED_CATEGORY
            elif input_job_status == Constants.IMPORT_FAILED:
                 return Constants.FAILED_CATEGORY
            elif input_job_status == Constants.FAILED:
                 return Constants.FAILED_CATEGORY
            elif input_job_status == Constants.IMPORTED_WITH_ERRORS:
                 return Constants.TRANSFER_ISSUES
            return None
       
    def get_sqs_queue_calling(self, original_arn:str)->str:

        if not original_arn:
            return ""
        # arn:aws:sqs:region:account-id:queue-name
        parts = original_arn.split(':')
        region = parts[3]
        account_id = parts[4]
        queue_name_only = parts[5]

        return queue_name_only
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
                transfer_return = {}
                if queue_name == Constants.CASE_SHARE_RECEIVED_SQS:
                    transfer_return = notifier.process_case_share_received(record)
                
                elif queue_name == Constants.TRANSFER_COMPLETED_SQS:
                    transfer_return = notifier.process_transfer_completed(record)
                    if not transfer_return:
                         logger.log_error(event=Constants.PROCESS_NAME,error=Exception("Commpleted event bridge not set"), job_id=job_id)
                    else:
                        job_id = transfer_return.get('job_id')
                elif queue_name == Constants.TRANSFER_EXCEPTION_INITIAL_EXCEP_SQS:
                     transfer_return = notifier.notify_first_exception_failure(job_id, record)
                elif queue_name == Constants.TRANSFER_EXCEPTION_SQS:
                     transfer_return = notifier.process_transfer_exception_sqs(record)

                # Pass the full record (or just the values) to your processor
                #notifier.process_message(record)
            
            logger.log_success(
                event="Transfer Notifier End",
                message="Successfully completed execution",
                job_id=context.aws_request_id
            )
            return transfer_return
            

        except Exception as e:
            logger.log_error(event="Lambda Execution Failed", error=str(e), job_id=getattr(context, 'aws_request_id', 'unknown'))
            raise