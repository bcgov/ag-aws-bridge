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

from datetime import datetime
from botocore.config import Config

from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import DatabaseManager, StatusCodes, get_db_manager


class Constants:
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    ERROR = "ERROR",
    PROCESS_NAME = "demsImportRequester"
    REGION_NAME = "ca-central-1"
    AGENCY_LOOKUP_TABLE_NAME = "agency-lookups"
    NOTIFICATION_MATRIX = "notification-distribution"
    HTTP_OK_CODE = 200,
    IMPORT_REQUESTED= "IMPORT-REQUESTED"
    SUCCESS_TRANSFER_QUEUE_NAME = ""
    TRANSFER_EXCEPTION_QUEUE_NAME = ""
    NOTIFY_SOURCE_AGENCY = "notify_source_on_complete"
    NOTIFY_BCPS = "notify_bcps_on_complete"
    NOTIFY_SYS_ADMIN = "notify_sysadmin_on_complete"


class TransferProcessExceptionHandler:
    """Main class to distribute indication of a success or an issue/failure/exception from anywhere in the BRIDGE process"""

    def __init__(self, env_stage: str, logger: LambdaStructuredLogger, job_id: str):
        """Initialize the validator with environment stage, logger, and job ID."""
        self.env_stage = env_stage
        self.logger = logger
        self.job_id = job_id
        self.ssm_client, self.sqs_client, self.agency_code_table, self.notification_matrix, self.email_client = self._initialize_aws_clients()
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
            boto3.resource("dynamodb", region_name=Constants.REGION_NAME).Table(Constants.NOTIFICATION_MATRIX),
            boto3.client('ses', region_name=Constants.REGION_NAME)
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
            f'/{self.env_stage}/bridge/notifications/notify_source_on_complete',
            f"/{self.env_stage}/bridge/notifications/notify_bcps_on_complete",
            f"/{self.env_stage}/bridge/notifications/notify_sysadmin_on_complete",
            f'/{self.env_stage}/bridge/notifications/notify_source_on_exception',
            f"/{self.env_stage}/bridge/notifications/notify_bcps_on_exception",
            f"/{self.env_stage}/bridge/notifications/notify_sysadmin_on_exception",
            f"/{self.env_stage}/bridge/notifications/notify_bcps_address",
            f"/{self.env_stage}/bridge/notifications/from_address",
            f"/{self.env_stage}/bridge/notifications/notify_sysadmin_address",
            f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception',
            f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import'
        ]
        
        try:

            parameters = {}
            batch_size = 10
            for i in range(0, len(parameter_names), batch_size):
                batch = parameter_names[i:i + batch_size]
                ssm_response = self.ssm_client.get_parameters(Names=batch, WithDecryption=True)
        
           
           # Only raise if there are actual invalid parameters
            if 'InvalidParameters' in ssm_response and len(ssm_response['InvalidParameters']) > 0:
                raise ValueError(f"Invalid parameters: {ssm_response['InvalidParameters']}")
        
            for param in ssm_response['Parameters']:
                parameters[param['Name']] = param['Value']
                #ssm_response = self.ssm_client.get_parameters(Names=parameter_names, WithDecryption=True)
                parameters = {param['Name']: param['Value'] for param in ssm_response['Parameters']}
            
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
    
    def update_evidence_files_import_requested(self, status:str, job_id:str, last_modified_process:str):
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

                    return_values = self.db_manager.bulk_update_evidence_file_states(file_status_updates_tuple,last_modified_process)
                    if return_values['success']:
                        self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Updated evidence files for job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                    else:
                        self.logger.log_error(event="Evidence File update Failed", error=str(e), job_id=self.job_id)

    def get_sqs_queue_calling(self, original_arn:str)->str:

        if not original_arn:
            return ""
        # arn:aws:sqs:region:account-id:queue-name
        parts = original_arn.split(':')
        region = parts[3]
        account_id = parts[4]
        queue_name_only = parts[5]

        return queue_name_only
    
    def get_attr(self, name:str, attrs:dict):
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
    def detectNotifyExceptionValues(self)->dict:
        '''Detect the notification flag set'''
       
        flagDict = {
            Constants.NOTIFY_SOURCE_AGENCY : False,
            Constants.NOTIFY_BCPS : False,
            Constants.NOTIFY_SYS_ADMIN : False
        }
        #grab notify source value
        notifySourceFlag = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_source_on_exception']
        notifyBcpsFlag = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_bcps_on_exception"]
        notifySysAdmin = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_sysadmin_on_exception"]

        if notifySourceFlag and notifySourceFlag == "True":
            flagDict[Constants.NOTIFY_SOURCE_AGENCY] = True
        elif notifyBcpsFlag and notifyBcpsFlag == "True":
            flagDict[Constants.NOTIFY_BCPS] = True
        elif notifySysAdmin and notifySysAdmin == "True":
             flagDict[Constants.NOTIFY_SYS_ADMIN] = True
        
        return flagDict
    
    def process_exception_queue_message(self, message:dict):
       
        receipt_handle = message['receiptHandle']
        messageId = message["messageId"]
       
        try:
            attrs = message.get('messageAttributes', {})
            job_id = self.get_attr('job_id',attrs)
            if not job_id:
                self.logger.log_error(
                event="Retrieving Job Id failed",
                message=f"Failed to parse message attributes for Job_id, ending process",
                error=None,
                job_id="unknown",
                message_id=messageId
                )
                return  # Abort processing early
            evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
            if evidence_transfer_job:
                # grab the job_status_code
                job_status_code = evidence_transfer_job["job_status_code"]
                destination = ""
                if job_status_code:
                    #grab the code value id
                    dynamo_response = self.notification_matrix.get_item(Key={'JobStatusCodeId': job_status_code})
                    if dynamo_response:
                        item = dynamo_response.get('Item')
                        if item :
                            item.get('notificationAddress')
                            if item.get('SendToBCPS') == 'Y':
                                destination = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_bcps_address"]
                                returnEmail = self.sendEmail(job_id, destination_email= destination, triggerQueue=Constants.TRANSFER_EXCEPTION_QUEUE_NAME)
                            elif item.get("SendToSysAdmin") == 'Y':
                                 destination = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_sysadmin_address"]
                                 returnEmail = self.sendEmail(job_id, destination_email= destination, triggerQueue=Constants.TRANSFER_EXCEPTION_QUEUE_NAME)
                            elif item.get("SendToAgency") == 'Y':
                                self.sendSourceAgency(job_id,triggerQueue=Constants.TRANSFER_EXCEPTION_QUEUE_NAME)
            if returnEmail:
                self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                
                self.update_evidence_files_import_requested(Constants.COMPLETED, job_id,   last_modified_process= "lambda: transfer process notifier")
                self.update_job_status(job_id,Constants.COMPLETED,"lambda: transfer process notifier")

                 # delete message
                exception_queue_url = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-transfer-exception']
              
                self.sqs_client.delete_message(QueueUrl = exception_queue_url, ReceiptHandle=receipt_handle)
        except Exception as e:
                self.logger.log_error(
                event="Message Parsing Failed",
                message=f"Failed to parse message attributes for MessageId: {messageId}",
                error=str(e),
                job_id="unknown",
                message_id=messageId
                )
                return  # Abort processing early
            
    def process_success_queue_message(self, message:dict):
        receipt_handle = message['receiptHandle']
        messageId = message["messageId"]
       
        try:
            attrs = message.get('messageAttributes', {})
            job_id = self.get_attr('job_id')
            if not job_id:
                self.logger.log_error(
                event="Retrieving Job Id failed",
                message=f"Failed to parse message attributes for Job_id, ending process",
                error=None,
                job_id="unknown",
                message_id=messageId
                )
                return  # Abort processing early
            #check if notification flags set
            flags = self.detectNotifyCompleteValues()

            if any(flags.values):
                # some notification was set
                for key,value  in flags.items():
                    if value:
                        if key == Constants.NOTIFY_SOURCE_AGENCY:
                            returnEmail = self.sendSourceAgency(job_id, triggerQueue=Constants.SUCCESS_TRANSFER_QUEUE_NAME)
                            
                        elif key == Constants.NOTIFY_BCPS:
                            destination = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_bcps_on_complete"]
                            returnEmail = self.sendEmail(job_id, destination_email=destination, triggerQueue=Constants.SUCCESS_TRANSFER_QUEUE_NAME)
                        elif key == Constants.NOTIFY_SYS_ADMIN:
                             destination = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_sysadmin_address"]
                             returnEmail = self.sendEmail(job_id, destination_email= destination, triggerQueue=Constants.SUCCESS_TRANSFER_QUEUE_NAME)
                            
                if returnEmail:
                     self.logger.log_success(event=Constants.PROCESS_NAME, message=f"Sent email successfully for  job_id: {job_id},  environment: {self.env_stage}", job_id=job_id)
                
                self.update_evidence_files_import_requested(Constants.COMPLETED, job_id,   last_modified_process= "lambda: transfer process notifier")
                self.update_job_status(job_id,Constants.COMPLETED,"lambda: transfer process notifier")

                 # delete message
                success_queue_url = self.parameters[f'/{self.env_stage}/bridge/sqs-queues/url_q-dems-import']
              
                self.sqs_client.delete_message(QueueUrl = success_queue_url, ReceiptHandle=receipt_handle)

        except Exception as e:
                self.logger.log_error(
                event="Message Parsing Failed",
                message=f"Failed to parse message attributes for MessageId: {messageId}",
                error=str(e),
                job_id="unknown",
                message_id=messageId
                )
                return  # Abort processing early

    def successCompletionEmailText(self,evidence_transfer_job:Dict):
         current_timestamp = datetime.now()
         body_text = ""
         subject_text = ""
         html_text = ""
         if evidence_transfer_job:
                    subject_text = "Axon Agency to BCPS DEMS Case Share - Completed -" + evidence_transfer_job["source_case_title"]
                    body_text = f"""The following Axon to BCPS DEMS Case Share has completed successfully: - Source Case Title ={evidence_transfer_job["source_case_title"]}
                                - Shared On = {evidence_transfer_job["source_case_last_modified_utc"]}
                                - Completed On = {current_timestamp.strftime("%Y-%m-%d %H:%M:%S")}
                                - Evidence File Count (received) = {evidence_transfer_job["source_case_evidence_count_to_download"]}
                                - Evidence File Count (transferred) = {evidence_transfer_job["source_case_evidence_count_downloaded"]}
                                - BCPS DEMS Case ID = {evidence_transfer_job["dems_case_id"]}
                                - BCPS (JUSTIN) Agency ID Code = {evidence_transfer_job["agency_id_code"]}
                                - BCPS (JUSTIN) Agency File Number = {evidence_transfer_job["agency_file_number"]}"""
                    
                    html_text = f"""<p>The following Axon to BCPS DEMS Case Share has completed successfully:</p>
                                    <ul>
                                        <li>Source Case Title = {evidence_transfer_job["source_case_title"]}</li>
                                        <li>Shared On = {evidence_transfer_job["source_case_last_modified_utc"]}</li>
                                        <li>Completed On = {current_timestamp.strftime("%Y-%m-%d %H:%M:%S")}</li>
                                        <li>Evidence File Count (received) = {evidence_transfer_job["source_case_evidence_count_to_download"]}</li>
                                        <li>Evidence File Count (transferred) = {evidence_transfer_job["source_case_evidence_count_downloaded"]}</li>
                                        <li>BCPS DEMS Case ID = {evidence_transfer_job["dems_case_id"]}</li>
                                        <li>BCPS (JUSTIN) Agency ID Code = {evidence_transfer_job["agency_id_code"]}</li>
                                        <li>BCPS (JUSTIN) Agency File Number = {evidence_transfer_job["agency_file_number"]}</li>
                                    </ul>"""

         return body_text, subject_text, html_text
    
    def exceptionEmailText(self, evidence_transfer_job:Dict):

         current_timestamp = datetime.now()
         body_text = ""
         subject_text = ""
         html_text = ""
         if evidence_transfer_job:
                    
                    count_evidence_files_imported = self.getEvidenceFileCountByStatus(fileStatus="IMPORTED", job_id=evidence_transfer_job["job_id"])
                    count_imported_errors = self.getEvidenceFileCountByStatus(fileStatus="IMPORTED-WITH-ERRORS", job_id=evidence_transfer_job["job_id"])
                    count_import_failed = self.getEvidenceFileCountByStatus(fileStatus="IMPORT-FAILED", job_id=evidence_transfer_job["job_id"])
                    exception_type=""
                    evidence_files = self.db_manager.get_evidence_files_by_job(job_id=evidence_transfer_job["job_id"])
                    ev_files_dems_error_messages = ""
                    ev_files_error_messages_html = ""
                    for item in evidence_files:
                        if item["dems_imported_error_message"]:
                            ev_files_dems_error_messages = ev_files_dems_error_messages + "\n" + item["dems_imported_error_message"]
                            ev_files_error_messages_html = ev_files_error_messages_html + "<br>" + item["dem_import_error_message"]

                    subject_text = "Axon Agency to BCPS DEMS Case Share - Exception - " + evidence_transfer_job["source_case_title"]
                    body_text = f"""The following Axon to BCPS DEMS Case Share encountered one or more exception(s):\n - Source Case Title ={evidence_transfer_job["source_case_title"]}
                                - Shared On = {evidence_transfer_job["source_case_last_modified_utc"]}
                                                            
                                - BCPS DEMS Case ID = {evidence_transfer_job["dems_case_id"] if evidence_transfer_job["dems_case_id"] else 'unknown' }
                                - BCPS (JUSTIN) Agency ID Code = {evidence_transfer_job["agency_id_code"] if evidence_transfer_job["agency_id_code"] else 'unknown'}
                                - BCPS (JUSTIN) Agency File Number = {evidence_transfer_job["agency_file_number"] if evidence_transfer_job["agency_file_number"] else 'unknown'}
                                - Exception Type = {exception_type}
                                - Exception Occurred = {current_timestamp.strftime("%Y-%m-%d %H:%M:%S")}
                                - Evidence File Count (received) = {evidence_transfer_job['source_case_evidence_count_to_download']}
                                - Evidence Files Count (successful) = {count_evidence_files_imported}
                                - Evidence Files Count (successful with errors) = {count_imported_errors}
                                - Evidence Files Count (import failed) = {count_import_failed} \n
                                - Exception Messaging:
                                    {ev_files_dems_error_messages}

                                """
                    html_text = f"""<p>The following Axon to BCPS DEMS Case Share encountered one or more exception(s):</p>
                            <ul>
                            <li>Source Case Title = {evidence_transfer_job["source_case_title"]}</li>
                            <li>Shared On = {evidence_transfer_job["source_case_last_modified_utc"]}</li>
                            <li>BCPS DEMS Case ID = {evidence_transfer_job["dems_case_id"] if evidence_transfer_job["dems_case_id"] else 'unknown' }</li>
                            <li>BCPS (JUSTIN) Agency ID Code = {evidence_transfer_job["agency_id_code"] if evidence_transfer_job["agency_id_code"] else 'unknown'}</li>
                            <li>BCPS (JUSTIN) Agency File Number = {evidence_transfer_job["agency_file_number"] if evidence_transfer_job["agency_file_number"] else 'unknown'}</li>
                            <li>Exception Type = {exception_type}</li>
                            <li>Exception Occurred = {current_timestamp.strftime("%Y-%m-%d %H:%M:%S")}</li>
                            <li>Evidence File Count (received) = {evidence_transfer_job['source_case_evidence_count_to_download']}</li>
                            <li>Evidence Files Count (successful) = {count_evidence_files_imported}</li>
                            <li>Evidence Files Count (successful with errors) = {count_imported_errors}</li>
                            <li>Evidence Files Count (import failed) = {count_import_failed}</li>
                            <li>Exception Messaging:<br>{ev_files_error_messages_html}</li>
                            </ul>"""

         return body_text, subject_text, html_text
    
    def getEvidenceFileCountByStatus(self, fileStatus :str, job_id:str)->int:
        if not fileStatus or not job_id :
            return 0
        
        queryStr = f'select count(evidence_id) as file_count from evidence_files where evidence_transfer_state_code=(select identifier from status_codes where value=%s) and job_id=%s'
        params = {  fileStatus,job_id}   
        try:
            queryResults = self.db_manager.execute_query(queryStr, params)
            return int(queryResults["file_count"])
        except Exception as e:
            self.logger.log_error(
                event=Constants.PROCESS_NAME,
                message=f"Failed to execute query for job_id: {job_id} and transfer file status : {fileStatus}",
                error=str(e),
                job_id=job_id,
                message_id="unknownn"
                )
            return 0  # Abort processing early

    def sendEmail(self, job_id:str, destination_email:str, source_email:str, triggerQueue:str)->bool:
        try:
               
            if job_id:
                evidence_transfer_job = self.db_manager.get_evidence_transfer_job(job_id)
                if triggerQueue:
                    if triggerQueue == Constants.SUCCESS_TRANSFER_QUEUE_NAME:
                       body_text, subject_text, html_body_text = self.successCompletionEmailText(evidence_transfer_job=evidence_transfer_job)
                    elif triggerQueue == Constants.TRANSFER_EXCEPTION_QUEUE_NAME :
                        body_text, subject_text, html_body_text = self.exceptionEmailText(evidence_transfer_job)

                    response = self.email_client.send_email( 
                        Source=source_email,  # Must be verified
                        Destination={'ToAddresses': [destination_email]},
                        Message={
                        'Subject': {'Data': subject_text},
                        'Body': {
                        'Text': {'Data': body_text},
                        'Html': {'Data': html_body_text} if html_body_text else None
                            }
                        }
                        )
                    if response:
                        try:
                            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                                return True
                        except Exception as emailError:
                            self.logger.log_error(
                            event="Sending email failed.",
                            message=f"Failed to send email for : {job_id}",
                            error=str(emailError),
                            job_id=job_id,
                            message_id="unknown"
                            )
                        return False  # Abort processing early
        except Exception as e:
                self.logger.log_error(
                event="Sending email failed.",
                message=f"Failed to send email for : {job_id}",
                error=str(e),
                job_id=job_id,
                message_id="unknown"
                )
                return False # Abort processing early

    def sendSourceAgency(self, job_id:str, triggerQueue:str)->bool:
            try:
                source_email_address = self.parameters[f"/{self.env_stage}/bridge/notifications/from_address"]
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
                message=f"Failed to send email for JobId: {job_id}",
                error=str(e),
                job_id=job_id,
                message_id="unknown"
                )
                return return_value # Abort processing early

    def detectNotifyCompleteValues(self)->dict:
        '''Detect the notification flag set'''
       
        flagDict = {
            Constants.NOTIFY_SOURCE_AGENCY : False,
            Constants.NOTIFY_BCPS : False,
            Constants.NOTIFY_SYS_ADMIN : False
        }
        #grab notify source value
        notifySourceFlag = self.parameters[f'/{self.env_stage}/bridge/notifications/notify_source_on_complete']
        notifyBcpsFlag = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_bcps_on_complete"]
        notifySysAdmin = self.parameters[f"/{self.env_stage}/bridge/notifications/notify_sysadmin_on_complete"]

        if notifySourceFlag and notifySourceFlag == "True":
            flagDict[Constants.NOTIFY_SOURCE_AGENCY] = True
        elif notifyBcpsFlag and notifyBcpsFlag == "True":
            flagDict[Constants.NOTIFY_BCPS] = True
        elif notifySysAdmin and notifySysAdmin == "True":
             flagDict[Constants.NOTIFY_SYS_ADMIN] = True
        
        return flagDict

def detectTransferExceptionValues(self)->dict:
     flagDict = {
            Constants.NOTIFY_SOURCE_AGENCY : False,
            Constants.NOTIFY_BCPS : False,
            Constants.NOTIFY_SYS_ADMIN : False
        }
     return flagDict

def lambda_handler(event, context):
    """Main Lambda handler function."""
    env_stage = os.environ.get('ENV_STAGE', 'dev-test')
    logger = LambdaStructuredLogger()
    logger.log_start(event="DEMS Import Requester Start", job_id=context.aws_request_id)
    
    try:
        requester = TransferProcessExceptionHandler(env_stage, logger, context.aws_request_id)

        if not event.get("Records"):
            logger.log(event="SQS Poll", status=Constants.IN_PROGRESS, 
                       message="No messages in queue", job_id=context.aws_request_id)
            return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'No messages to process'}

        for record in event["Records"]:
            attrs = record.get('messageAttributes', {})
            queue_arn = record['eventSourceARN']
            queue_name = requester.get_sqs_queue_calling(queue_arn)

            if queue_name == Constants.SUCCESS_TRANSFER_QUEUE_NAME:
                requester.process_success_queue_message(record)
                
            elif queue_name == Constants.TRANSFER_EXCEPTION_QUEUE_NAME:
                requester.process_exception_queue_message(record)
           
        logger.log_success(
            event="Dems Import Requester End",
            message="Successfully completed execution",
            job_id=context.aws_request_id
        )
        return {'statusCode': Constants.HTTP_OK_CODE, 'body': 'Processing complete'}

    except Exception as e:
        logger.log_error(event="Lambda Execution Failed", error=str(e), job_id=getattr(context, 'aws_request_id', 'unknown'))
        raise