import csv
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import time
from typing import Dict, Any, List, Optional
import zipfile

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import urllib3
import json as json_lib
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import get_db_manager, StatusCodes
from sts_credential_manager import STSCredentialManager

ssm_client = boto3.client("ssm")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AXON Evidence Data Transferor Lambda function for processing a single SQS message with the job_id and transfer the prepared package to EDT target location via the AWS SDK.

    Args:
        event: SQS event containing single message
        context: Lambda context object

    Returns:
        Dict with processing results
    """

    # Initialize the logger
    logger = LambdaStructuredLogger()

    # Extract request information
    request_id = context.aws_request_id

    # Get environment stage from environment variable
    env_stage = os.environ.get("ENV_STAGE", "dev-test")

    # Base context data for all log entries
    base_context = {
        "request_id": request_id,
        "function_name": context.function_name,
        "env_stage": env_stage,
    }
    
    job_id = None
    message_id = None

    try:
        # Log the start of the function
        logger.log_start(event="axon_evidence_data_transferor", job_id=request_id)

        records = event.get("Records", [])
        record: dict = records[0]
        message_id = record.get("messageId")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_message_id",
            context_data={
                "env_stage": env_stage,
                "message_id": f"Processing message id {message_id}",
            },
        )

        # Parse the message body
        message_body = json.loads(record.get("body", "{}"))
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_message_body",
            context_data={
                "env_stage": env_stage,
                "message_body": f"Message content: {message_body}",
            },
        )

        # Validate message body has job id
        job_id = message_body.get("job_id")
        if not job_id:
            raise ValueError(f"Invalid Job ID: {job_id}")

        # Retrieve SSM parameters
        ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

        # Retrieve source case information
        db_manager = get_db_manager(env_param_in=env_stage)
        case_info = db_manager.get_evidence_transfer_job(job_id)
        if not case_info:
            raise ValueError(f"Invalid Case Information: {case_info}")
        
        dems_case_id = case_info.get('dems_case_id')
        if dems_case_id is None:
            raise ValueError(f"Invalid DEMS Case ID: {dems_case_id}")
        
        source_case_title = case_info.get('source_case_title')
        if not source_case_title:
            raise ValueError(f"Invalid source_case_title: {source_case_title}")
        
        source_case_id = case_info.get('source_case_id')
        if not source_case_id:
            raise ValueError(f"Invalid source_case_id: {source_case_id}")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_case_info",
            context_data={
                "env_stage": env_stage,
                "dems_case_id": f"{dems_case_id}",
                "source_case_title": source_case_title,
                "source_case_id": source_case_id,
            },
        )

        # Assume EDT's role
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_assuming_edt_role",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        # Initialize STS Credential Manager (uses Lambda's current role credentials)
        cred_manager = STSCredentialManager(region_name='ca-central-1')
        
        # Assume EDT's role using job_id - duration of 1800 seconds (30 minutes)
        temp_credentials = cred_manager.assume_role_with_job_id(
            job_id=job_id,
            duration=1800
        )
        
        if not temp_credentials:
            error_msg = f"Failed to assume EDT DEMS role for job_id: {job_id}"
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_transferor_assume_role_failed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "error": error_msg,
                },
            )
            raise RuntimeError(error_msg)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_edt_role_assumed",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "assumed_role_arn": temp_credentials['AssumedRoleArn'],
                "credentials_expire": temp_credentials['Expiration'].isoformat(),
            },
        )
        
        # Get source and destination S3 information
        source_bucket = ssm_parameters['bridge_s3_bucket']

        # Construct source_key using the same pattern as transfer_file_to_s3
        # Pattern: {{source_case_title}_{dems_case_id}_{job_id}.zip
        folder_name = f"{source_case_title}_{dems_case_id}_{job_id}"
        source_key = f"{folder_name}.zip"
        source_key = "PO-2025-99008_5271_d9281a42-5d0f-4ced-8bbf-357adb7de364.zip"

        dest_bucket = ssm_parameters['edt_s3_bucket']
        # dest_key = source_key
        edt_bucket_subfolder = ssm_parameters.get('edt_s3_bucket_subfolder', '').strip('/')
        dest_key = f"{edt_bucket_subfolder}/{source_key}"

        if not source_bucket or not dest_bucket:
            raise ValueError("Missing S3 bucket configuration in SSM parameters")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_s3_transfer_start",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "source_bucket": source_bucket,
                "source_key": source_key,
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
            },
        )
        
        # Transfer evidence package to EDT S3
        transfer_result = transfer_evidence_to_edt(
            source_bucket=source_bucket,
            source_key=source_key,
            dest_bucket=dest_bucket,
            dest_key=dest_key,
            temp_credentials=temp_credentials,
            logger=logger,
            event=event,
            env_stage=env_stage,
            job_id=job_id
        )
        
        if not transfer_result['success']:
            raise RuntimeError(f"S3 transfer failed: {transfer_result.get('error')}")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_s3_transfer_complete",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_size_bytes": transfer_result.get('file_size'),
                "transfer_method": transfer_result.get('method'),
                "dest_location": f"s3://{dest_bucket}/{dest_key}",
            },
        )

        # Update tracking database
        db_update_result = update_transfer_tracking_database(
            job_id=job_id,
            db_manager=db_manager,
            logger=logger,
            event=event,
            env_stage=env_stage
        )

        if not db_update_result['success']:
            raise RuntimeError(f"Database update failed: {db_update_result.get('error')}")

        complete_load_file_path = f"{edt_bucket_subfolder}/{source_key}/{transfer_result.get('load_file_path')}"
        # Queue success message with CSV path
        queue_success_message(
            job_id=job_id,
            success_queue_url=ssm_parameters['dems_import_queue_url'],
            loadFilePath=complete_load_file_path,
            dems_case_id=dems_case_id
        )

        return {
            "statusCode": 200,
            "message": f"Successfully processed message {message_id}",
            "job_id": job_id,
            "transfer_details": transfer_result
        }

    except Exception as e:
        error_message = str(e)
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_transferor_error",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id if job_id else "unknown",
                "message_id": message_id if message_id else "unknown",
                "error": error_message,
            },
        )
        
        # Update database for failure if we have job_id
        if job_id:
            try:
                db_update_result = update_transfer_failure_database(
                    job_id=job_id,
                    error_message=error_message,
                    db_manager=db_manager,
                    logger=logger,
                    event=event,
                    env_stage=env_stage
                )
                logger.log(
                    event=event,
                    level=LogLevel.INFO,
                    status=LogStatus.SUCCESS if db_update_result['success'] else LogStatus.FAILURE,
                    message="axon_evidence_data_transferor_failure_database_update_complete",
                    context_data={
                        "env_stage": env_stage,
                        "job_id": job_id,
                        "db_update_success": db_update_result['success'],
                    },
                )
            except Exception as db_error:
                logger.log(
                    event=event,
                    level=LogLevel.ERROR,
                    status=LogStatus.FAILURE,
                    message="axon_evidence_data_transferor_failure_database_update_error",
                    context_data={
                        "env_stage": env_stage,
                        "job_id": job_id,
                        "error": str(db_error),
                    },
                )

            # Queue error message
            error_queue_url = ssm_parameters.get("transfer_exception_queue_url")
            if error_queue_url:
                if queue_error_message(job_id, error_queue_url, error_details=error_message):
                    logger.log(
                        event=event,
                        level=LogLevel.INFO,
                        status=LogStatus.SUCCESS,
                        message="axon_evidence_data_transferor_queued_error",
                        context_data={
                            "env_stage": env_stage,
                            "job_id": job_id,
                            "queue_url": error_queue_url,
                        },
                    )        
        return {
            "statusCode": 500, 
            "error": f"Failed to process message {message_id if message_id else 'unknown'}",
            "details": error_message
        }


def get_ssm_parameters(
    env_stage, logger: LambdaStructuredLogger, event, context_data=None
):
    """
    Retrieve required SSM parameters for the given environment stage.

    Args:
        env_stage (str): Environment stage (e.g., 'dev', 'prod')
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        context_data (dict): Additional context for logging

    Returns:
        dict: Dictionary containing the retrieved parameters
    """
    if context_data is None:
        context_data = {}

    # Define parameter paths
    parameter_paths = {
        "edt_s3_bucket": f"/{env_stage}/edt/s3/bucket",
        "edt_s3_bucket_subfolder": f"/{env_stage}/edt/s3/bucket-subfolder",
        "bridge_s3_bucket": f"/{env_stage}/bridge/s3/bridge-transient-data-transfer-s3",
        "dems_import_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-dems-import",
        "transfer_exception_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-transfer-exception"
    }

    parameters = {}
    failed_parameters = []
    start_time = time.time()

    try:
        # Get all parameters in batch (more efficient than individual calls)
        parameter_names = list(parameter_paths.values())

        response = ssm_client.get_parameters(
            Names=parameter_names,
            WithDecryption=True,  # This will decrypt SecureString parameters
        )

        # Calculate response time
        response_time_ms = (time.time() - start_time) * 1000

        # Process successful parameters
        for param in response.get("Parameters", []):
            param_name = param["Name"]
            param_value = param["Value"]

            # Find the logical name for this parameter path
            logical_name = None
            for logical, path in parameter_paths.items():
                if path == param_name:
                    logical_name = logical
                    break

            if logical_name:
                parameters[logical_name] = param_value

        # Process failed parameters
        failed_parameters = response.get("InvalidParameters", [])

        if failed_parameters:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="log_ssm_parameter_collection",
                context_data={
                    **context_data,
                    "env_stage": env_stage,
                    "failed_parameters": failed_parameters,
                    "retrieved_parameters": list(parameters.keys()),
                    "operation": "ssm_parameter_retrieval_partial_failure",
                },
            )

            raise ValueError(f"Failed to retrieve SSM parameters: {failed_parameters}")

        # Log retrieval status
        logger.log_ssm_parameter_collection(
            parameter_names=list(parameters.keys()),
            parameters_collected=parameters,
            response_time_ms=response_time_ms,
            invalid_parameters=failed_parameters if failed_parameters else None,
            **context_data,
        )

        return parameters

    except ClientError as e:
        response_time_ms = (time.time() - start_time) * 1000
        error_code = e.response["Error"]["Code"]
        error_message = e.response["Error"]["Message"]

        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_ssm_parameter_collection",
            context_data={
                **context_data,
                "env_stage": env_stage,
                "error_code": error_code,
                "error_message": error_message,
                "failed_parameters": parameter_names,
                "operation": "ssm_parameter_retrieval_client_error",
                "response_time_ms": response_time_ms,
            },
        )

        raise Exception(f"SSM Client Error: {error_code} - {error_message}")

    except Exception as e:
        response_time_ms = (time.time() - start_time) * 1000
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="log_ssm_parameter_collection",
            context_data={
                **context_data,
                "env_stage": env_stage,
                "error": str(e),
                "error_type": type(e).__name__,
                "operation": "ssm_parameter_retrieval_unexpected_error",
                "response_time_ms": response_time_ms,
            },
        )

        raise


def transfer_evidence_to_edt(
    source_bucket: str,
    source_key: str,
    dest_bucket: str,
    dest_key: str,
    temp_credentials: Dict[str, Any],
    logger,
    event: Dict[str, Any],
    env_stage: str,
    job_id: str
) -> Dict[str, Any]:
    """
    Transfer evidence package from BRIDGE S3 to EDT S3.
    Validates CSV exists in zip, extracts CSV path, and transfers the file.
    Requires download/upload approach since EDT credentials don't have access to BRIDGE S3 bucket.
    
    Returns:
        Dict with success status, file info, csv_path, and transfer details
    """
    
    local_path = None
    
    try:
        # Create S3 client for source bucket (using Lambda's execution role)
        source_s3_client = boto3.client('s3', region_name='ca-central-1')
        
        # Create S3 client for destination bucket (using EDT's temporary credentials)
        dest_s3_client = boto3.client(
            's3',
            aws_access_key_id=temp_credentials['AccessKeyId'],
            aws_secret_access_key=temp_credentials['SecretAccessKey'],
            aws_session_token=temp_credentials['SessionToken'],
            region_name='ca-central-1'
        )
        
        # Get source object metadata
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_getting_file_metadata",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "source_bucket": source_bucket,
                "source_key": source_key,
            },
        )
        
        head_response = source_s3_client.head_object(
            Bucket=source_bucket,
            Key=source_key
        )
        file_size = head_response['ContentLength']
        file_size_gb = file_size / (1024 ** 3)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_file_size_determined",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_size_bytes": file_size,
                "file_size_gb": round(file_size_gb, 2),
            },
        )
        
        # Configure transfer settings for efficient multipart operations
        config = TransferConfig(
            multipart_threshold=100 * 1024 * 1024,  # 100MB - use multipart for files > 100MB
            max_concurrency=10,
            multipart_chunksize=100 * 1024 * 1024,  # 100MB chunks
            use_threads=True
        )
        
        # Download from source (BRIDGE bucket with intermediate credentials)
        local_path = f"/tmp/{job_id}_{Path(source_key).name}"
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_downloading_from_source",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "local_path": local_path,
                "file_size_gb": round(file_size_gb, 2),
            },
        )
        
        source_s3_client.download_file(
            source_bucket,
            source_key,
            local_path,
            Config=config
        )
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_download_complete",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        # Extract and validate CSV path from zip
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_extracting_csv_path",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        csv_path = None
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_transferor_zip_contents_listed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "file_count": len(file_list),
                },
            )
            
            # Search for CSV files
            csv_files = [f for f in file_list if f.lower().endswith('.csv')]
            
            if not csv_files:
                error_msg = f"No CSV file found in zip archive. Files: {file_list}"
                logger.log(
                    event=event,
                    level=LogLevel.ERROR,
                    status=LogStatus.FAILURE,
                    message="axon_evidence_data_transferor_no_csv_found",
                    context_data={
                        "env_stage": env_stage,
                        "job_id": job_id,
                        "error": error_msg,
                    },
                )
                raise ValueError(error_msg)
            
            # Use the first CSV file found
            csv_path = csv_files[0]
            
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_transferor_csv_path_extracted",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "csv_path": csv_path,
                    "csv_count": len(csv_files),
                },
            )
        
        # Upload to destination (EDT's bucket with EDT's credentials)
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_uploading_to_dest",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dest_bucket": dest_bucket,
                "dest_key": dest_key,
            },
        )
        
        dest_s3_client.upload_file(
            local_path,
            dest_bucket,
            dest_key,
            Config=config
        )
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_upload_complete",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        # Clean up temporary file
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_transferor_tmp_file_cleaned",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                },
            )
        
        # Verify the file exists in destination
        dest_head = dest_s3_client.head_object(
            Bucket=dest_bucket,
            Key=dest_key
        )
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_transfer_verified",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dest_file_size": dest_head['ContentLength'],
                "source_file_size": file_size,
                "sizes_match": dest_head['ContentLength'] == file_size,
            },
        )
        
        return {
            'success': True,
            'file_size': file_size,
            'file_size_gb': round(file_size_gb, 2),
            'method': 'download_upload',
            'source': f"s3://{source_bucket}/{source_key}",
            'destination': f"s3://{dest_bucket}/{dest_key}",
            'load_file_path': csv_path
        }
        
    except zipfile.BadZipFile as e:
        error_message = f"Invalid or corrupted zip file: {str(e)}"
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_transferor_bad_zip_file",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "error": error_message,
            },
        )
        return {
            'success': False,
            'error': error_message
        }
        
    except Exception as e:
        error_message = str(e)
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_transferor_transfer_failed",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "error": error_message,
            },
        )
        
        return {
            'success': False,
            'error': error_message
        }
    
    finally:
        # Clean up temp file if it still exists
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception as cleanup_error:
                logger.log(
                    event=event,
                    level=LogLevel.WARNING,
                    status=LogStatus.SUCCESS,
                    message="axon_evidence_data_transferor_final_cleanup_warning",
                    context_data={
                        "env_stage": env_stage,
                        "job_id": job_id,
                        "error": str(cleanup_error),
                    },
                )
    

def queue_message(
    job_id: str, 
    queue_url: str, 
    is_error: bool = False
) -> bool:
    """
    Queue the job_id for the next lambda to pick up or send message to exception queue.
    
    Args:
        job_id: The job ID to queue
        queue_url: The SQS queue URL to send the message to
        is_error: If True, send to error queue with bridge_job_id; if False, send to success queue with job_id
        
    Returns:
        bool: True if message was successfully queued, False otherwise
    """
    try:        
        sqs_client = boto3.client('sqs', region_name='ca-central-1')

        # Prepare message body based on queue type
        if is_error:
            message_body = {
                'bridge_job_id': job_id,
            }
            message_attributes = {
                'bridge_job_id': {
                    'StringValue': job_id,
                    'DataType': 'String'
                },
            }
        else:
            message_body = {
                'job_id': job_id,
            }
            message_attributes = {
                'job_id': {
                    'StringValue': job_id,
                    'DataType': 'String'
                },
            }
        
        # Prepare message for FIFO queue
        message_params = {
            'QueueUrl': queue_url,
            'MessageBody': json.dumps(message_body),
            'MessageGroupId': f"job-{job_id}",
            'MessageDeduplicationId': f"{job_id}-{int(time.time())}",
            'MessageAttributes': message_attributes
        }
        
        # Send message to queue
        response = sqs_client.send_message(**message_params)
        
        message_id = response.get('MessageId')
        queue_type = "error" if is_error else "success"
        print(f"Successfully queued message to {queue_type} SQS queue. MessageId: {message_id}")
        return True
        
    except Exception as e:
        queue_type = "error" if is_error else "success"
        print(f"Error queuing message to {queue_type} SQS queue: {str(e)}")
        return False


def update_transfer_tracking_database(
    job_id: str,
    db_manager,
    logger,
    event: Dict[str, Any],
    env_stage: str
) -> Dict[str, Any]:
    """
    Update tracking database after successful evidence transfer to EDT.
    Updates both evidence_transfer_jobs and evidence_files tables.
    
    Args:
        job_id: The job identifier
        db_manager: Database manager instance
        logger: Logger instance
        event: Lambda event for logging
        env_stage: Environment stage
    
    Returns:
        Dict with update results including success status
    """
    
    try:
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_updating_database",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        # Step 1: Update evidence_transfer_jobs table
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_updating_job_record",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        job_update_result = db_manager.update_job_status(
            job_id=job_id,
            status_code=StatusCodes.TRANSFERRED,
            job_msg='Files transferred to DEMS S3 destination',
            last_modified_process='lambda: data transferor'
        )
        
        if not job_update_result:
            error_msg = f"Failed to update evidence_transfer_jobs for job_id: {job_id}"
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_transferor_job_update_failed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "error": error_msg,
                },
            )
            return {
                'success': False,
                'error': error_msg
            }
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_job_record_updated",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "status_code": StatusCodes.TRANSFERRED,
            },
        )
        
        # Step 3: Get all evidence files for this job
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_fetching_evidence_files",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        evidence_files = db_manager.get_evidence_files_by_job(job_id)
        
        if not evidence_files:
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_transferor_no_evidence_files_found",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                },
            )
            return {
                'success': True,
                'job_updated': True,
                'files_updated': 0
            }
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_evidence_files_found",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_count": len(evidence_files),
            },
        )
        
        # Step 4: Prepare bulk update list
        evidence_updates = []
        for evidence_file in evidence_files:
            evidence_id = evidence_file.get('evidence_id')
            evidence_updates.append((evidence_id, StatusCodes.TRANSFERRED))
        
        # Step 5: Bulk update evidence files with atomic transaction
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_bulk_updating_evidence_files",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_count": len(evidence_updates),
            },
        )
        
        bulk_update_result = db_manager.bulk_update_evidence_file_states(
            evidence_updates=evidence_updates,
            last_modified_process='lambda: data transferor'
        )
        
        if not bulk_update_result.get('success'):
            error_msg = f"Failed to bulk update evidence files: {bulk_update_result.get('error')}"
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_transferor_bulk_update_failed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "error": error_msg,
                },
            )
            return {
                'success': False,
                'error': error_msg,
                'job_updated': True,
                'files_updated': bulk_update_result.get('updated_count', 0)
            }
        
        files_updated = bulk_update_result.get('updated_count', 0)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_all_records_updated",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "job_updated": True,
                "files_updated": files_updated,
                "status_code": StatusCodes.TRANSFERRED,
            },
        )
        
        return {
            'success': True,
            'job_updated': True,
            'files_updated': files_updated
        }
    
    except Exception as e:
        error_message = str(e)
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_transferor_database_update_exception",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "error": error_message,
            },
        )
        
        return {
            'success': False,
            'error': error_message
        }
    
def update_transfer_failure_database(
    job_id: str,
    error_message: str,
    db_manager,
    logger,
    event: Dict[str, Any],
    env_stage: str
) -> Dict[str, Any]:
    """
    Update tracking database when evidence transfer fails.
    Updates both evidence_transfer_jobs and evidence_files tables to FAILED status.
    
    Args:
        job_id: The job identifier
        error_message: Error message describing the failure
        db_manager: Database manager instance
        logger: Logger instance
        event: Lambda event for logging
        env_stage: Environment stage
    
    Returns:
        Dict with update results including success status
    """
    
    try:
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_updating_failure_database",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "error": error_message,
            },
        )
        
        # Step 1: Update evidence_transfer_jobs table to FAILED
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_updating_job_failed_status",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        job_update_result = db_manager.update_job_status(
            job_id=job_id,
            status_code=StatusCodes.FAILED,
            job_msg=f"Files transfer to DEMS S3 failed: {error_message}",
            last_modified_process='lambda: data transferor'
        )
        
        if not job_update_result:
            error_msg = f"Failed to update evidence_transfer_jobs FAILED status for job_id: {job_id}"
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_transferor_job_failed_update_failed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "error": error_msg,
                },
            )
            return {
                'success': False,
                'error': error_msg
            }
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_job_failed_status_updated",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "status_code": StatusCodes.FAILED,
            },
        )
        
        # Step 2: Get all evidence files for this job
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_fetching_evidence_files_for_failure",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
            },
        )
        
        evidence_files = db_manager.get_evidence_files_by_job(job_id)
        
        if not evidence_files:
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_transferor_no_evidence_files_for_failure",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                },
            )
            return {
                'success': True,
                'job_updated': True,
                'files_updated': 0
            }
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_evidence_files_found_for_failure",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_count": len(evidence_files),
            },
        )
        
        # Step 3: Prepare bulk update list for FAILED status
        evidence_updates = []
        for evidence_file in evidence_files:
            evidence_id = evidence_file.get('evidence_id')
            evidence_updates.append((evidence_id, StatusCodes.FAILED))
        
        # Step 4: Bulk update evidence files to FAILED with atomic transaction
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.IN_PROGRESS,
            message="axon_evidence_data_transferor_bulk_updating_evidence_files_failed",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "file_count": len(evidence_updates),
            },
        )
        
        bulk_update_result = db_manager.bulk_update_evidence_file_states(
            evidence_updates=evidence_updates,
            last_modified_process='lambda: data transferor'
        )
        
        if not bulk_update_result.get('success'):
            error_msg = f"Failed to bulk update evidence files to FAILED: {bulk_update_result.get('error')}"
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_transferor_bulk_update_failed_status",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "error": error_msg,
                },
            )
            return {
                'success': False,
                'error': error_msg,
                'job_updated': True,
                'files_updated': bulk_update_result.get('updated_count', 0)
            }
        
        files_updated = bulk_update_result.get('updated_count', 0)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_transferor_all_failure_records_updated",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "job_updated": True,
                "files_updated": files_updated,
                "status_code": StatusCodes.FAILED,
            },
        )
        
        return {
            'success': True,
            'job_updated': True,
            'files_updated': files_updated
        }
    
    except Exception as e:
        error_message_inner = str(e)
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_transferor_failure_database_update_exception",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "error": error_message_inner,
            },
        )
        
        return {
            'success': False,
            'error': error_message_inner
        }
    
def queue_message(
    job_id: str, 
    queue_url: str,
    message_data: Optional[Dict[str, Any]] = None,
    is_error: bool = False
) -> bool:
    """
    Queue a message to SQS for the next lambda or exception handling.
    
    Args:
        job_id: The job ID (used for message grouping and deduplication)
        queue_url: The SQS queue URL to send the message to
        message_data: Optional dictionary of additional data to include in the message body
                     Example: {'loadFilePath': '/path/to/file', 'dems_case_id': 'ABC'}
        is_error: If True, sends to error queue; if False, sends to success queue
        
    Returns:
        bool: True if message was successfully queued, False otherwise
    """
    try:        
        sqs_client = boto3.client('sqs', region_name='ca-central-1')
        
        # Build message body with job_id and any additional data
        body = {'job_id': job_id}
        if message_data:
            body.update(message_data)
        
        # Build message attributes with job_id and all data elements
        message_attributes = {
            'job_id': {
                'StringValue': job_id,
                'DataType': 'String'
            }
        }
        
        if message_data:
            for key, value in message_data.items():
                message_attributes[key] = {
                    'StringValue': str(value),
                    'DataType': 'String'
                }
        
        # Prepare message for FIFO queue
        message_params = {
            'QueueUrl': queue_url,
            'MessageBody': json.dumps(body),
            'MessageGroupId': f"job-{job_id}",
            'MessageDeduplicationId': f"{job_id}-{int(time.time())}",
            'MessageAttributes': message_attributes
        }
        
        # Send message to queue
        response = sqs_client.send_message(**message_params)
        
        message_id = response.get('MessageId')
        queue_type = "error" if is_error else "success"
        print(f"Successfully queued message to {queue_type} SQS queue. MessageId: {message_id}")
        return True
        
    except Exception as e:
        queue_type = "error" if is_error else "success"
        print(f"Error queuing message to {queue_type} SQS queue: {str(e)}")
        return False


def queue_success_message(
    job_id: str, 
    success_queue_url: str,
    loadFilePath: Optional[str] = None,
    dems_case_id: Optional[str] = None,
    **additional_data
) -> bool:
    """
    Queue message for successful evidence transfer.
    
    Args:
        job_id: The job ID
        success_queue_url: The success SQS queue URL
        loadFilePath: Optional path to the loaded file
        dems_case_id: Optional DEMS case ID
        **additional_data: Any other data elements to include
        
    Returns:
        bool: True if message was successfully queued, False otherwise
    """
    message_data = {}
    
    if loadFilePath is not None:
        message_data['loadFilePath'] = loadFilePath
    
    if dems_case_id is not None:
        message_data['dems_case_id'] = dems_case_id
    
    # Add any additional data elements
    message_data.update(additional_data)
    
    return queue_message(job_id, success_queue_url, message_data if message_data else None, is_error=False)


def queue_error_message(
    job_id: str, 
    error_queue_url: str,
    error_details: Optional[str] = None,
    **additional_data
) -> bool:
    """
    Queue message for failed evidence transfer.
    
    Args:
        job_id: The job ID
        error_queue_url: The error SQS queue URL
        error_details: Optional error message/details
        **additional_data: Any other data elements to include
        
    Returns:
        bool: True if message was successfully queued, False otherwise
    """
    message_data = {}
    
    if error_details is not None:
        message_data['error_details'] = error_details
    
    # Add any additional data elements
    message_data.update(additional_data)
    
    return queue_message(job_id, error_queue_url, message_data if message_data else None, is_error=True)
