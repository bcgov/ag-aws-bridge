import csv
from datetime import datetime
import hashlib
import json
import os
import time
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError
import urllib3
import json as json_lib
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import get_db_manager, StatusCodes
from esl_processor.processor import ESLProcessor
from app.transfer_package_manager import TransferPackageManager

ssm_client = boto3.client("ssm")


def fetch_share_logs(agency_id: str, case_id: str, base_url: str, bearer_token: str, logger: Any, event: Dict[str, Any], env_stage: str) -> Dict[str, Any]:
    """Fetch share logs from Axon API."""
    url = f"{base_url}api/v3/agencies/{agency_id}/cases/{case_id}/relationships/share-logs?shareType=0"
    print(f"url {url}")
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    http = urllib3.PoolManager()
    try:
        response = http.request('GET', url, headers=headers, timeout=30.0)
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="fetch_share_logs_response",
            context_data={"env_stage": env_stage, "status": response.status, "data_length": len(response.data) if response.data else 0}
        )
        if response.status == 404:
            # No share logs found, return empty data
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="fetch_share_logs_not_found",
                context_data={"env_stage": env_stage, "status": response.status, "message": "No share logs found for the case"}
            )
            return {"data": []}
        if response.status != 200:
            raise urllib3.exceptions.HTTPError(f"HTTP {response.status}: {response.data.decode('utf-8')}")
        return json_lib.loads(response.data.decode('utf-8'))
    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="fetch_share_logs_error",
            context_data={"error": str(e), "env_stage": env_stage}
        )
        raise


def generate_share_log_report(agency_id: str, case_id: str, share_log_id: str, base_url: str, bearer_token: str, logger: Any, event: Dict[str, Any], env_stage: str) -> bool:
    """Generate share log report if not exists."""
    url = f"{base_url}api/v3/agencies/{agency_id}/cases/{case_id}/relationships/share-logs/{share_log_id}/share-log-reports"
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    body = json_lib.dumps({
        "data": {
            "type": "shareLogReport",
            "attributes": {
                "format": "csv"
            }
        }
    })
    http = urllib3.PoolManager()
    try:
        response = http.request('POST', url, headers=headers, body=body, timeout=30.0)
        print(f"POST call to {url} body: {body}")

        response_data = json_lib.loads(response.data.decode('utf-8')) if response.data else {}
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="generate_share_log_report_response",
            context_data={"env_stage": env_stage, "status": response.status, "share_log_id": share_log_id, "response": response_data}
        )
        if "errors" in response_data and any("detail" in error for error in response_data.get("errors", [])):
            raise urllib3.exceptions.HTTPError(f"API Error: {json_lib.dumps(response_data['errors'])}")
        if response.status not in [200, 500]:  # 500 if already generated
            raise urllib3.exceptions.HTTPError(f"HTTP {response.status}: {response.data.decode('utf-8')}")
        return response.status == 200
    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="generate_share_log_report_error",
            context_data={"error": str(e), "env_stage": env_stage}
        )
        raise


def download_share_log_csv(agency_id: str, case_id: str, share_log_id: str, report_id: str, base_url: str, bearer_token: str, output_path: str, logger: Any, event: Dict[str, Any], env_stage: str) -> bool:
    """Download share log CSV."""
    url = f"{base_url}api/v3/agencies/{agency_id}/cases/{case_id}/relationships/share-logs/{share_log_id}/share-log-reports/{report_id}"
    headers = {
        'Authorization': f'Bearer {bearer_token}',
    }
    http = urllib3.PoolManager()
    try:
        print(f"Downloading share log CSV from {url} to {output_path}")
        response = http.request('GET', url, headers=headers, timeout=30.0)
        if response.status != 200:
            raise urllib3.exceptions.HTTPError(f"HTTP {response.status}: {response.data.decode('utf-8')}")
        with open(output_path, 'wb') as f:
            f.write(response.data)
        return True
    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="download_share_log_csv_error",
            context_data={"error": str(e), "env_stage": env_stage}
        )
        raise


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AXON Evidence Data Preparator Lambda function for processing a single SQS message to generate ESL.

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
        logger.log_start(event="axon_evidence_data_preparator", job_id=request_id)

        records = event.get("Records", [])
        record: dict = records[0]
        message_id = record.get("messageId")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_message_id",
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
            message="axon_evidence_data_preparator_message_body",
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

        # Retrieve source case title
        db_manager = get_db_manager(env_param_in=env_stage)
        case_info = db_manager.get_source_case_information(job_id)
        if not case_info:
            raise ValueError(f"Invalid Case Information: {case_info}")
        source_case_title = case_info.get('source_case_title')
        if source_case_title is None:
            raise ValueError(f"Invalid Source Case Title: {source_case_title}")
        source_case_id = case_info.get('source_case_id')
        if source_case_id is None:
            raise ValueError(f"Invalid Source Case ID: {source_case_id}")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_source_case_title",
            context_data={
                "env_stage": env_stage,
                "source_case_title": f"{source_case_title}",
            },
        )

        # Retrieve source agency
        source_agency = db_manager.get_source_agency(job_id)
        if source_agency is None:
            raise ValueError(f"Invalid Source Agency: {source_agency}")
        agency_id = source_agency
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_source_agency",
            context_data={
                "env_stage": env_stage,
                "source_agency": f"{source_agency}",
            },
        )

        # Fetch share logs
        share_logs_response = fetch_share_logs(agency_id, source_case_id, ssm_parameters["base_url"], ssm_parameters["bearer"], logger, event, env_stage)
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="share_logs_response_received",
            context_data={"env_stage": env_stage, "response_type": type(share_logs_response).__name__, "has_data": "data" in share_logs_response if isinstance(share_logs_response, dict) else False}
        )
        share_logs = share_logs_response.get("data", [])
        if not share_logs:
            raise ValueError("No share logs found")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="share_logs_found",
            context_data={"env_stage": env_stage, "share_logs_count": len(share_logs), "first_log_type": type(share_logs[0]).__name__ if share_logs else "none"}
        )
        
        # Find most recent share log
        most_recent = max(share_logs, key=lambda x: x["attributes"]["createdDate"])
        share_log_id = most_recent["id"]
        print(f"Most recent share log ID: {share_log_id}")
        share_log_name = most_recent["attributes"]["name"]
        share_log_reports = most_recent.get("relationships", {}).get("shareLogReports", {}).get("data", [])
        
        if share_log_reports:
            report_id = share_log_reports[0]["id"]
            print(f"Using existing report ID: {report_id}")
        else:
            # Generate report
            generate_share_log_report(agency_id, source_case_id, share_log_id, ssm_parameters["base_url"], ssm_parameters["bearer"], logger, event, env_stage)
            # Re-fetch
            share_logs_response = fetch_share_logs(agency_id, source_case_id, ssm_parameters["base_url"], ssm_parameters["bearer"], logger, event, env_stage)
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="share_logs_response_received",
                context_data={"env_stage": env_stage, "response_type": type(share_logs_response).__name__, "has_data": "data" in share_logs_response if isinstance(share_logs_response, dict) else False}
            )   
            most_recent = max(share_logs_response.get("data", []), key=lambda x: x["attributes"]["createdDate"])
            share_log_reports = most_recent.get("relationships", {}).get("shareLogReports", {}).get("data", [])
            if not share_log_reports:
                raise ValueError("Failed to generate share log report")
            report_id = share_log_reports[0]["id"]
        
        # Download CSV
        input_csv_path = f"/tmp/{share_log_name}.csv"
        download_share_log_csv(agency_id, source_case_id, share_log_id, report_id, ssm_parameters["base_url"], ssm_parameters["bearer"], input_csv_path, logger, event, env_stage)

        # Generate timestamp in YYMMDDHHMMSS format
        timestamp = datetime.now().strftime('%y%m%d%H%M%S')
        
        # Create filename: ESL_source_case_title_YYMMDDHHMMSS.csv
        filename = f"ESL_{source_case_title}_{timestamp}.csv"
        filepath = f"/tmp/{filename}"

        # Get agency_id_code from DB for this job
        job_details = db_manager.get_evidence_transfer_job(job_id)
        agency_id_code = job_details.get("agency_id_code") if job_details else None
        if not agency_id_code:
            raise ValueError(f"Missing agency_id_code for job_id {job_id}")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_agency_id_code",
            context_data={
                "env_stage": env_stage,
                "agency_id_code": agency_id_code,
                "job_id": job_id,
            },
        )

        # Process with ESLProcessor
        processor = ESLProcessor(
            db_manager=db_manager,
            agency_id_code=agency_id_code,
            logger=logger,
            event=event,
            base_url=ssm_parameters["base_url"],
            bearer_token=ssm_parameters["bearer"],
            agency_id=agency_id
        )
        success, message, csv_evidence_id_map = processor.process(
            input_csv_path,
            filepath,
            job_id
        )
        if not success:
            raise Exception(f"ESLProcessor failed: {message}")

        # Verify file was created and get file size
        if not os.path.exists(filepath):
            raise Exception(f"Output file was not created successfully at {filepath}")
            
        file_size = os.path.getsize(filepath)

        csv_result = {
            'filename': filename,
            'filepath': filepath,
            'file_size': file_size,
            'records_count': None,  # Could parse from message if needed
            'creation_timestamp': timestamp
        }

        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_csv_generated",
            context_data={
                "env_stage": env_stage,
                "csv_filename": csv_result.get("filename"),
                "csv_filepath": csv_result.get("filepath"),
                "file_size": csv_result.get("file_size"),
            },
        )

        # Stream-zip all evidence files from S3 + CSV from local /tmp into one zip
        tpm = TransferPackageManager(logger=logger, env_stage=env_stage)
        zip_result = tpm.create_and_upload_zip_stream(
            ssm_parameters=ssm_parameters,
            case_info=case_info,
            job_id=job_id,
            csv_filename=csv_result['filename'],
            csv_filepath=csv_result['filepath'],
            event=event,
        )
        if not zip_result.get('success'):
            raise Exception(f"Stream-zip to S3 failed: {zip_result.get('error')}")

        logger.log(
            event=event,
            level=1,  # LogLevel.INFO
            status=1,  # LogStatus.SUCCESS
            message="transfer_package_zipped_success",
            context_data={
                "job_id": job_id,
                "zip_s3_uri": zip_result.get('zip_s3_uri'),
            },
        )

        # Update job status to IMPORT_FILE_GENERATED
        job_updated = db_manager.update_job_status(
            job_id=job_id,
            status_code=StatusCodes.IMPORT_FILE_GENERATED,
            job_msg="Transfer package has been generated",
            last_modified_process="lambda: evidence data preparator"
        )    
        
        # Update all evidence_files to mark as TRANSFER_READY and populate evidence_id_source
        if job_updated:
            evidence_files = db_manager.get_evidence_files_by_job(job_id)
            for evidence_file in evidence_files:
                evidence_id = evidence_file.get('evidence_id')
                if evidence_id:
                    # Get evidence_id from CSV if available
                    checksum = evidence_file.get('checksum', '').lower()
                    evidence_id_source = csv_evidence_id_map.get(checksum)
                    
                    if not evidence_id_source:
                        logger.log(
                            event=event,
                            level=LogLevel.WARNING,
                            status=LogStatus.WARNING,
                            message="axon_evidence_data_preparator_missing_csv_evidence_id",
                            context_data={
                                "env_stage": env_stage,
                                "evidence_id": evidence_id,
                                "checksum": checksum,
                            },
                        )
                    
                    db_manager.update_evidence_file_state(
                        evidence_id,
                        StatusCodes.TRANSFER_READY,
                        "lambda: evidence data preparator",
                        evidence_id_source=evidence_id_source
                    )

        # Queue success message
        success_queue_url = ssm_parameters["transfer_prepare_queue_url"]
        source_path = zip_result.get('zip_s3_uri')
        if queue_success_message(job_id, success_queue_url, source_path=source_path):
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_preparator_queued_success",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "queue_url": success_queue_url,
                    "source_path": source_path,
                },
            )
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_success",
            context_data={
                "env_stage": env_stage,
                "message": f"evidence share log prepared: {job_id}",
            },
        )

        return {
            "statusCode": 200,
            "message": f"Successfully processed message {message_id}",
        }

    except Exception as e:
        error_message = str(e)
        
        # Update database for failure scenario
        if job_id:
            try:
                db_manager = get_db_manager(env_param_in=env_stage)
                
                # Update job status to FAILED
                db_manager.update_job_status(
                    job_id=job_id,
                    status_code=StatusCodes.FAILED,
                    job_msg=error_message,
                    last_modified_process="lambda: evidence data preparator"
                )
                
                # Update all evidence_files to FAILED state and populate evidence_id_source if available
                evidence_files = db_manager.get_evidence_files_by_job(job_id)
                for evidence_file in evidence_files:
                    evidence_id = evidence_file.get('evidence_id')
                    if evidence_id:
                        # Get evidence_id from CSV if available (if we got far enough to build the map)
                        evidence_id_source = None
                        if 'csv_evidence_id_map' in locals():
                            checksum = evidence_file.get('checksum', '').lower()
                            evidence_id_source = csv_evidence_id_map.get(checksum)
                        
                        db_manager.update_evidence_file_state(
                            evidence_id,
                            StatusCodes.FAILED,
                            "lambda: evidence data preparator",
                            evidence_id_source=evidence_id_source
                        )
                        
            except Exception as db_error:
                logger.log(
                    event=event,
                    level=LogLevel.ERROR,
                    status=LogStatus.FAILURE,
                    message="axon_evidence_data_preparator_db_update_error",
                    context_data={
                        "env_stage": env_stage,
                        "error": str(db_error),
                    },
                )
        
        # Queue error message
        if job_id and 'ssm_parameters' in locals():
            error_queue_url = ssm_parameters.get("transfer_exception_queue_url")
            if error_queue_url:
                if queue_error_message(job_id, error_queue_url):
                    logger.log(
                        event=event,
                        level=LogLevel.INFO,
                        status=LogStatus.SUCCESS,
                        message="axon_evidence_data_preparator_queued_error",
                        context_data={
                            "env_stage": env_stage,
                            "job_id": job_id,
                            "queue_url": error_queue_url,
                        },
                    )
        
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_preparator_error",
            context_data={
                "env_stage": env_stage,
                "error_message": f"Failed to process message {message_id if message_id else 'unknown'}",
                "error": error_message,
                "error_type": type(e).__name__,
                "job_id": job_id if job_id else "unknown",
            },
        )
        logger.log_error(event=f"preparator_fail_{job_id}", error=e)


        return {
            "statusCode": 500, 
            "error": f"Failed to process message {message_id if message_id else 'unknown'}"
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
        "bearer": f"/{env_stage}/axon/api/bearer",
        "base_url": f"/{env_stage}/axon/api/base_url",
        "transfer_prepare_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-data-transfer",
        "transfer_exception_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-transfer-exception",
        "bridge_s3_bucket": f"/{env_stage}/bridge/s3/bridge-transient-data-transfer-s3",
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


def generate_csv_log_file(
    job_id: str,
    source_case_title: str,
    logger: Any,
    event: Dict[str, Any],
    env_stage: str,
    ssm_parameters: Dict[str, str],
) -> Dict[str, Any]:
    """Generate CSV log file with ESL naming format.
    
    Args:
        job_id: The job ID from SQS message
        source_case_title: Retrieved from database
        logger: Logger instance
        event: Lambda event
        env_stage: Environment stage
        
    Returns:
        Dict with file generation results
    """
    
    try:
        # Generate timestamp in YYMMDDHHMMSS format
        timestamp = datetime.now().strftime('%y%m%d%H%M%S')
        
        # Create filename: ESL_source_case_title_YYMMDDHHMMSS.csv
        filename = f"ESL_{source_case_title}_{timestamp}.csv"
        filepath = f"/tmp/{filename}"
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="axon_evidence_data_preparator_csv_creation_started",
            context_data={
                "env_stage": env_stage,
                "filename": filename,
                "filepath": filepath,
                "job_id": job_id,
            },
        )
        
        # Prepare CSV data
        csv_data = prepare_csv_data(job_id, source_case_title, logger, event, env_stage, ssm_parameters)

        # Create CSV file
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            if csv_data and isinstance(csv_data, list) and len(csv_data) > 0:
                # Use keys from first record as headers
                fieldnames = csv_data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                # Write header
                writer.writeheader()
                
                # Write data rows
                for row in csv_data:
                    writer.writerow(row)
            else:
                # Create empty CSV with default headers if no data
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'job_id', 'source_case_title', 'event_type', 'message'])
                # Write at least one row with basic info
                writer.writerow([
                    datetime.now().isoformat(),
                    job_id,
                    source_case_title,
                    'INFO',
                    'CSV log file initialized'
                ])
        
        # Verify file was created and get file size
        if not os.path.exists(filepath):
            raise Exception(f"File was not created successfully at {filepath}")
            
        file_size = os.path.getsize(filepath)
        
        return {
            'filename': filename,
            'filepath': filepath,
            'file_size': file_size,
            'records_count': len(csv_data) if csv_data else 1,
            'creation_timestamp': timestamp
        }
        
    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="axon_evidence_data_preparator_csv_creation_error",
            context_data={
                "env_stage": env_stage,
                "error": str(e),
                "job_id": job_id,
            },
        )
        raise


def prepare_csv_data(
    job_id: str,
    source_case_title: str,
    logger: Any,
    event: Dict[str, Any],
    env_stage: str,
    ssm_parameters: Dict[str, str],
) -> List[Dict[str, Any]]:
    """Prepare data for CSV file.
    
    Args:
        job_id: The job ID
        source_case_title: Source case title
        logger: Logger instance
        event: Lambda event
        env_stage: Environment stage
        ssm_parameters: SSM parameters including base_url and bearer token
        
    Returns:
        List of dictionaries representing CSV rows
    """
    
    # Data preparation logic
    db_manager = get_db_manager(env_param_in=env_stage)
    # Retrieve agency
    source_agency = db_manager.get_source_agency(job_id)
    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="axon_evidence_data_preparator_source_agency",
        context_data={
            "env_stage": env_stage,
            "source_agency": f"{source_agency}",
        },
    )
    if source_agency is None:
        raise ValueError(f"Invalid Source Agency: {source_agency}")

    # Retrieve all evidence files for a job
    evidence_files = db_manager.get_evidence_files_by_job(job_id)
    if not evidence_files or len(evidence_files) == 0:
        raise ValueError(f"Not able to find Evidence files: {evidence_files}")

    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="axon_evidence_data_preparator_evidence_files_count",
        context_data={
            "env_stage": env_stage,
            "evidence_files_count": len(evidence_files),
        },
    )

    base_url = ssm_parameters["base_url"]
    bearer_token = ssm_parameters["bearer"]
    http = urllib3.PoolManager()
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }

    csv_data = []
    
    # Loop through all evidence files
    for idx, evidence_file in enumerate(evidence_files):
        evidence_id = evidence_file.get('evidence_id')
        evidence_file_id = evidence_file.get('evidence_file_id')
        if evidence_id is None:
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.WARNING,
                message="axon_evidence_data_preparator_missing_evidence_id",
                context_data={
                    "env_stage": env_stage,
                    "file_index": idx,
                    "evidence_id": str(evidence_id),
                },
            )
            continue
            
        try:
            # Construct evidence URL
            evidence_url = construct_evidence_url(base_url, source_agency, evidence_id)
            
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_preparator_api_call_start",
                context_data={
                    "env_stage": env_stage,
                    "evidence_id": evidence_id,
                    "evidence_url": evidence_url,
                    "file_index": f"{idx + 1}/{len(evidence_files)}",
                },
            )
            
            # Make API call to Axon
            response = http.request('GET', evidence_url, headers=headers, timeout=30.0)
            
            # Check if request was successful
            if response.status != 200:
                raise urllib3.exceptions.HTTPError(f"HTTP {response.status}: {response.data.decode('utf-8')}")
            # Parse JSON response
            api_data = json_lib.loads(response.data.decode('utf-8'))
            
            # Extract data from response
            evidence_data = api_data.get('data', {})
            attributes = evidence_data.get('attributes', {})
            
            title = attributes.get('title', 'N/A')
            modified_on = attributes.get('modifiedOn', 'N/A')
            # Check EIM compliance
            is_eim_compliant, eim_parts = check_eim_compliance(title)

            # Get file name by matching evidence_file_id in files_data
            relationships = evidence_data.get('relationships', {})
            files_data = relationships.get('files', {}).get('data', [])
            file_name = None
            file_name_no_ext = None
            for file_data in files_data:
                if file_data.get('id') == evidence_file_id:
                    file_attributes = file_data.get('attributes', {})
                    file_name = file_attributes.get('fileName')
                    file_name_no_ext = os.path.splitext(file_name)[0]
                    break

            # If no matching file found, skip this record
            if not file_name:
                logger.log(
                    event=event,
                    level=LogLevel.WARNING,
                    status=LogStatus.WARNING,
                    message="axon_evidence_data_preparator_file_not_found",
                    context_data={
                        "env_stage": env_stage,
                        "evidence_id": evidence_id,
                        "evidence_file_id": evidence_file_id,
                    },
                )
                continue            
            # Parse modifiedOn date from UTC to YYYYMMDD format
            date_to_crown = parse_utc_to_date(modified_on)
            if is_eim_compliant:
                csv_row = {
                    'ID': f"{source_case_title}.{title}",
                    'TYPE': '',
                    'DESCRIPTION': eim_parts.get('description', ''),
                    'TITLE': eim_parts.get('title', ''),
                    'DATE': eim_parts.get('date', ''),
                    'DATE TO CROWN': date_to_crown,
                    'RELATIVE FILE PATH': file_name,
                    'DISCLOSED STATUS': '',
                    'ORIGINAL FILE NUMBER': source_case_title
                }
            else:
                csv_row = {
                    'ID': f"{source_case_title}.{file_name_no_ext}",
                    'TYPE': '',
                    'DESCRIPTION': file_name_no_ext,  # Full original filename without extension
                    'TITLE': '',
                    'DATE': '',
                    'DATE TO CROWN': date_to_crown,
                    'RELATIVE FILE PATH': file_name,
                    'DISCLOSED STATUS': '',
                    'ORIGINAL FILE NUMBER': source_case_title
                }
            
            csv_data.append(csv_row)
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="axon_evidence_data_preparator_file_processed",
                context_data={
                    "env_stage": env_stage,
                    "evidence_id": evidence_id,
                    "file_name": file_name,
                    "eim_compliant": is_eim_compliant,
                },
            )
            
        except (urllib3.exceptions.HTTPError, urllib3.exceptions.TimeoutError, urllib3.exceptions.RequestError) as api_error:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_preparator_api_call_error",
                context_data={
                    "env_stage": env_stage,
                    "evidence_id": evidence_id,
                    "error": str(api_error),
                    "file_index": f"{idx + 1}/{len(evidence_files)}",
                },
            )
            
        except Exception as parse_error:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="axon_evidence_data_preparator_parse_error",
                context_data={
                    "env_stage": env_stage,
                    "evidence_id": evidence_id,
                    "error": str(parse_error),
                    "file_index": f"{idx + 1}/{len(evidence_files)}",
                },
            )
    
    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="axon_evidence_data_preparator_csv_data_prepared",
        context_data={
            "env_stage": env_stage,
            "records_prepared": len(csv_data),
            "job_id": job_id,
            "evidence_files_processed": len(evidence_files),
        },
    )
    
    return csv_data


def construct_evidence_url(
    base_url: str, source_agency: str, evidence_id: str
) -> str:
    """
    Construct the full URL for accessing an evidence file.

    Args:
        base_url: axon base url
        source_agency: The source agency GUID
        evidence_id: The evidence ID GUID

    Returns:
        str: The constructed URL
    """
    base_url = base_url.rstrip("/")
    url = f"{base_url}/api/v2/agencies/{source_agency}/evidence/{evidence_id}"

    return url


def check_eim_compliance(filename: str) -> tuple[bool, Dict[str, str]]:
    """Check if a filename is EIM compliant and extract components.
    
    Parsing from RIGHT to LEFT:
    1. Check for TIME (HHMM - 4 digits) - OPTIONAL
    2. Check for DATE (YYMMDD - 6 digits) - REQUIRED for compliance
    3. Extract TITLE (rightmost remaining segment)
    4. Extract DESCRIPTION (everything else to the left)
    
    EIM Compliance requires: DESCRIPTION + TITLE + DATE (TIME is optional)
    
    For non-compliant files:
    - description: full original filename
    - title, date, time: empty strings
    
    Args:
        filename: Filename without extension
        
    Returns:
        tuple of (is_compliant, parts_dict)
        parts_dict contains: description, title, date, time (if present)
    """
    
    # Split by underscores
    parts = filename.split('_')
    
    # Must have at least 3 parts (DESCRIPTION_TITLE_DATE minimum)
    if len(parts) < 3:
        return False, {
            'description': filename,
            'title': '',
            'date': '',
        }
    
    # Parse from RIGHT to LEFT
    idx = len(parts) - 1
    potential_time = None
    potential_date = None
    
    # Step 1: Check for TIME (rightmost, 4 digits, HHMM format) - OPTIONAL
    if idx >= 0 and len(parts[idx]) == 4 and parts[idx].isdigit():
        if is_valid_time(parts[idx]):
            potential_time = parts[idx]
            idx -= 1  # Move left
    
    # Step 2: Check for DATE (6 digits, YYMMDD format) - REQUIRED
    if idx >= 0 and len(parts[idx]) == 6 and parts[idx].isdigit():
        if is_valid_date(parts[idx]):
            potential_date = parts[idx]
            idx -= 1  # Move left
    
    # If no valid date found, not EIM compliant
    if not potential_date:
        return False, {
            'description': filename,
            'title': '',
            'date': '',
        }
    
    # Step 3: Extract TITLE (the rightmost remaining segment)
    if idx < 0:
        # Not enough parts remaining
        return False, {
            'description': filename,
            'title': '',
            'date': '',
        }
    
    title = parts[idx].upper()
    
    # Step 4: DESCRIPTION is everything to the left of TITLE
    if idx == 0:
        # No description part
        return False, {
            'description': filename,
            'title': '',
            'date': '',
        }
    
    description_parts = parts[0:idx]
    description = '_'.join(description_parts).upper()
    
    # Build result - we know it's EIM compliant at this point
    result = {
        'description': description,
        'title': title,
    }
    
    # Add date (expand 6-digit to 8-digit)
    date_cleaned = potential_date.strip('()')
    expanded_date = f"20{date_cleaned}"
    result['date'] = expanded_date
    
    # Add time if present
    if potential_time:
        result['time'] = potential_time.strip('()')
    
    # EIM compliant: has DESCRIPTION + TITLE + DATE
    return True, result


def is_valid_date(date_str: str) -> bool:
    """Validate date string in YYMMDD or YYYYMMDD format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        True if valid date format
    """
    
    # Strip parentheses for validation
    date_cleaned = date_str.strip('()')
    
    # Check if it's all digits
    if not date_cleaned.isdigit():
        return False
    
    # Must be 6 or 8 digits
    if len(date_cleaned) not in [6, 8]:
        return False
    
    try:
        if len(date_cleaned) == 6:
            # YYMMDD format
            year = int(date_cleaned[0:2])
            month = int(date_cleaned[2:4])
            day = int(date_cleaned[4:6])
            
            # Year range 00-99
            if not (0 <= year <= 99):
                return False
        else:
            # YYYYMMDD format
            year = int(date_cleaned[0:4])
            month = int(date_cleaned[4:6])
            day = int(date_cleaned[6:8])
            
            # Year range 1900-2100
            if not (1900 <= year <= 2100):
                return False
        
        # Month range 01-12
        if not (1 <= month <= 12):
            return False
        
        # Day range 01-31
        if not (1 <= day <= 31):
            return False
        
        return True
        
    except (ValueError, IndexError):
        return False


def is_valid_time(time_str: str) -> bool:
    """Validate time string in HHMM format.
    
    Args:
        time_str: Time string to validate
        
    Returns:
        True if valid time format
    """
    
    # Strip parentheses for validation
    time_cleaned = time_str.strip('()')
    
    # Must be exactly 4 digits
    if not time_cleaned.isdigit() or len(time_cleaned) != 4:
        return False
    
    try:
        hour = int(time_cleaned[0:2])
        minute = int(time_cleaned[2:4])
        
        # Hour range 00-24
        if not (0 <= hour <= 24):
            return False
        
        # Minute range 00-59
        if not (0 <= minute <= 59):
            return False
        
        return True
        
    except (ValueError, IndexError):
        return False


def parse_utc_to_date(utc_timestamp: str) -> str:
    """Parse UTC timestamp to YYYYMMDD format.
    
    Args:
        utc_timestamp: UTC timestamp string (e.g., "2025-06-13T18:37:30.537904100Z")
        
    Returns:
        Date string in YYYYMMDD format, or empty string if parsing fails
    """
    
    if not utc_timestamp or utc_timestamp == 'N/A':
        return ''
    
    try:
        # Remove milliseconds/nanoseconds and Z suffix
        # Handle various timestamp formats
        timestamp_clean = utc_timestamp.split('.')[0].replace('Z', '')
        
        # Parse the timestamp
        dt = datetime.fromisoformat(timestamp_clean)
        
        # Return in YYYYMMDD format
        return dt.strftime('%Y%m%d')
        
    except (ValueError, AttributeError):
        return ''
    

def queue_message(
    job_id: str, 
    queue_url: str, 
    is_error: bool = False,
    source_path: str | None = None,
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
            if source_path:
                message_body['source_path'] = source_path
                message_attributes['source_path'] = {
                    'StringValue': source_path,
                    'DataType': 'String'
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


# Helper functions for clearer usage
def queue_success_message(job_id: str, success_queue_url: str, source_path: str | None = None) -> bool:
    """Queue message for successful CSV generation."""
    return queue_message(job_id, success_queue_url, is_error=False, source_path=source_path)


def queue_error_message(job_id: str, error_queue_url: str) -> bool:
    """Queue message for failed CSV generation."""
    return queue_message(job_id, error_queue_url, is_error=True)

