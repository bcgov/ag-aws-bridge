import json
import os
import time
from typing import Dict, Any

import boto3
from botocore.exceptions import ClientError
import urllib3
from lambda_structured_logger import LambdaStructuredLogger, LogLevel, LogStatus
from bridge_tracking_db_layer import get_db_manager, StatusCodes

ssm_client = boto3.client("ssm")
sqs_client = boto3.client("sqs")


def get_ssm_parameters(
    env_stage: str, logger: LambdaStructuredLogger, event: Dict[str, Any], context_data: Dict[str, Any] = None
) -> Dict[str, str]:
    """
    Retrieve required SSM parameters for the given environment stage.

    Args:
        env_stage: Environment stage (e.g., 'dev', 'prod')
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        context_data: Additional context for logging

    Returns:
        Dictionary containing the retrieved parameters
    """
    if context_data is None:
        context_data = {}

    # Define parameter paths
    parameter_paths = {
        "bearer": f"/{env_stage}/edt/api/bearer",
        "import_status_url": f"/{env_stage}/edt/api/import_status_url",
        "get_list_case_evidence_url": f"/{env_stage}/axon/api/get_list_case_evidence_url",
        "connection_string": f"/{env_stage}/bridge/tracking-db/connection-string",
        "transfer_exception_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-transfer-exception",
        "transfer_completion_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-axon-transfer-completion",
        "import_status_retries_queue_url": f"/{env_stage}/bridge/sqs-queues/url_q-dems-import-status",
        "max_retries": f"/{env_stage}/bridge/sqs-queues/lambda-dems-import-status-retries",
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
        raise


# Constants for DEMS Import Status Values
class DemsImportStatus:
    """DEMS Import Status Constants"""
    
    # Finished successfully
    COMPLETE = "Complete"
    
    # Finished unsuccessfully
    FAILED = "Failed"
    DELETED = "Deleted"
    REJECTED = "Rejected"
    CANCELLED = "Cancelled"
    COMPLETED_WITH_ERRORS = "Completed with errors"
    COMPLETED_WITH_WARNINGS = "Completed with warnings"
    COMPLETED_WITH_ERRORS_PREMATURELY = "Completed with errors prematurely"
    
    # Still processing (incomplete)
    QUEUED = "Queued"
    IMPORTING = "Importing"
    LOADING = "Loading"
    DELETING = "Deleting"
    REJECTING = "Rejecting"
    PROCESSING = "Processing"
    VALIDATING = "Validating"
    
    # Status sets for evaluation
    FINISHED_SUCCESSFUL = {COMPLETE}
    FINISHED_UNSUCCESSFUL = {
        FAILED, DELETED, REJECTED, CANCELLED,
        COMPLETED_WITH_ERRORS, COMPLETED_WITH_WARNINGS,
        COMPLETED_WITH_ERRORS_PREMATURELY
    }
    INCOMPLETE = {
        QUEUED, IMPORTING, LOADING, DELETING,
        REJECTING, PROCESSING, VALIDATING
    }


def construct_dems_url(base_url: str, dems_case_id: str, dems_import_job_id: str) -> str:
    """
    Construct the DEMS import status API URL by replacing placeholders.

    Args:
        base_url: Base URL from SSM parameter with $$$$ and #### placeholders
        dems_case_id: DEMS case ID to replace $$$$ with
        dems_import_job_id: DEMS import job ID to replace #### with

    Returns:
        Constructed full URL for the DEMS API endpoint
    """
    url = base_url.replace("$$$$", str(dems_case_id))
    url = url.replace("####", str(dems_import_job_id))
    return url


def call_dems_import_status_api(
    dems_case_id: str,
    dems_import_job_id: str,
    base_url: str,
    bearer_token: str,
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Call the EDT DEMS API to check the status of an import job.

    Args:
        dems_case_id: DEMS case ID
        dems_import_job_id: DEMS import job ID
        base_url: Base DEMS import status URL from SSM (with $$$$ and #### placeholders)
        bearer_token: Bearer token for authorization
        logger: LambdaStructuredLogger instance
        event: Lambda event object for logging
        env_stage: Environment stage for logging

    Returns:
        Full response data from the DEMS API (as dict)

    Raises:
        urllib3.exceptions.HTTPError: For connection errors or timeouts
        Exception: For other unexpected errors
    """
    # Construct the full URL
    url = construct_dems_url(base_url, dems_case_id, dems_import_job_id)

    # Prepare headers
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    http = urllib3.PoolManager()

    try:
        # Make the GET request to DEMS API
        response = http.request(
            "GET",
            url,
            headers=headers,
            timeout=30.0,
        )

        # Log the API call result
        response_body = response.data.decode("utf-8") if response.data else ""
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="call_dems_import_status_api_response",
            context_data={
                "env_stage": env_stage,
                "dems_case_id": dems_case_id,
                "dems_import_job_id": dems_import_job_id,
                "status_code": response.status,
                "response_body": response_body,
            },
        )

        # Parse and return the response
        try:
            return json.loads(response_body) if response_body else {}
        except json.JSONDecodeError:
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.SUCCESS,
                message="call_dems_import_status_api_invalid_json",
                context_data={
                    "env_stage": env_stage,
                    "dems_case_id": dems_case_id,
                    "dems_import_job_id": dems_import_job_id,
                    "response_body": response_body,
                },
            )
            return {"raw_response": response_body, "status_code": response.status}

    except (urllib3.exceptions.TimeoutError, urllib3.exceptions.ConnectionError) as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="call_dems_import_status_api_timeout_error",
            context_data={
                "env_stage": env_stage,
                "dems_case_id": dems_case_id,
                "dems_import_job_id": dems_import_job_id,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        raise

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="call_dems_import_status_api_error",
            context_data={
                "env_stage": env_stage,
                "dems_case_id": dems_case_id,
                "dems_import_job_id": dems_import_job_id,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        raise


def evaluate_and_handle_import_status(
    response_data: Dict[str, Any],
    job_id: str,
    dems_case_id: str,
    dems_import_job_id: str,
    source_path: str,
    import_name: str,
    attempt_number: int,
    max_retries: int,
    ssm_parameters: Dict[str, str],
    db_manager: Any,
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Evaluate the DEMS import status response and route to the appropriate handler.

    Args:
        response_data: Parsed DEMS API response
        job_id: Bridge tracking job ID
        dems_case_id: DEMS case ID
        dems_import_job_id: DEMS import job ID
        source_path: Source path of the import
        import_name: Import name from the original message
        attempt_number: Current retry attempt number
        max_retries: Maximum number of retries allowed
        ssm_parameters: SSM parameters dict
        db_manager: Database manager instance
        logger: LambdaStructuredLogger instance
        event: Lambda event object
        env_stage: Environment stage

    Returns:
        Result dictionary from the appropriate handler
    """
    status = response_data.get("status")
    record_count = response_data.get("recordCount")
    
    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="evaluate_import_status_start",
        context_data={
            "env_stage": env_stage,
            "job_id": job_id,
            "dems_import_job_id": dems_import_job_id,
            "status": status,
            "record_count": record_count,
            "attempt_number": attempt_number,
            "max_retries": max_retries,
        },
    )
    
    # Scenario 1: Import Complete (status = "Complete")
    if status in DemsImportStatus.FINISHED_SUCCESSFUL:
        # Get count of evidence_files where dems_is_transferred = true for this job_id
        db_transferred_count = db_manager.get_evidence_files_transferred_count(job_id)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="import_complete_count_comparison",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
                "db_transferred_count": db_transferred_count,
                "api_record_count": record_count,
                "counts_match": db_transferred_count == record_count,
            },
        )
        
        if db_transferred_count == record_count:
            # Happy Path: Import complete, no errors, recordCount matches DB count
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="import_complete_success_path",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "record_count": record_count,
                },
            )
            return handle_import_complete_success(
                response_data=response_data,
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                ssm_parameters=ssm_parameters,
                db_manager=db_manager,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
        else:
            # Alternate Path: recordCount doesn't match DB transferred count
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.SUCCESS,
                message="import_complete_count_mismatch",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "api_record_count": record_count,
                    "db_transferred_count": db_transferred_count,
                },
            )
            return handle_import_complete_with_errors(
                response_data=response_data,
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                ssm_parameters=ssm_parameters,
                db_manager=db_manager,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
    
    # Scenario 2: Import Complete with errors/warnings
    elif status in DemsImportStatus.FINISHED_UNSUCCESSFUL:
        logger.log(
            event=event,
            level=LogLevel.WARNING,
            status=LogStatus.SUCCESS,
            message="import_complete_with_errors_path",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
                "status": status,
            },
        )
        return handle_import_complete_with_errors(
            response_data=response_data,
            job_id=job_id,
            dems_case_id=dems_case_id,
            dems_import_job_id=dems_import_job_id,
            source_path=source_path,
            ssm_parameters=ssm_parameters,
            db_manager=db_manager,
            logger=logger,
            event=event,
            env_stage=env_stage,
        )
    
    # Scenario 3: Import still in progress
    elif status in DemsImportStatus.INCOMPLETE:
        if attempt_number < max_retries:
            logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="import_in_progress_retry",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "status": status,
                    "attempt_number": attempt_number,
                    "max_retries": max_retries,
                },
            )
            return handle_import_in_progress(
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                import_name=import_name,
                attempt_number=attempt_number,
                ssm_parameters=ssm_parameters,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
        else:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="import_in_progress_max_retries_reached",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "status": status,
                    "attempt_number": attempt_number,
                    "max_retries": max_retries,
                },
            )
            return handle_import_max_retries_reached(
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                ssm_parameters=ssm_parameters,
                db_manager=db_manager,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
    
    # Scenario 4: Import doesn't exist or invalid request
    else:
        if attempt_number < max_retries:
            logger.log(
                event=event,
                level=LogLevel.WARNING,
                status=LogStatus.SUCCESS,
                message="import_not_found_retry",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "status": status,
                    "attempt_number": attempt_number,
                    "max_retries": max_retries,
                },
            )
            return handle_import_in_progress(
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                import_name=import_name,
                attempt_number=attempt_number,
                ssm_parameters=ssm_parameters,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
        else:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="import_not_found_max_retries_reached",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "status": status,
                    "attempt_number": attempt_number,
                    "max_retries": max_retries,
                },
            )
            return handle_import_max_retries_reached(
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                ssm_parameters=ssm_parameters,
                db_manager=db_manager,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )


def handle_import_complete_success(
    response_data: Dict[str, Any],
    job_id: str,
    dems_case_id: str,
    dems_import_job_id: str,
    source_path: str,
    ssm_parameters: Dict[str, str],
    db_manager: Any,
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Handle successful import completion (Happy Path).
    
    Status: "Complete"
    Condition: recordCount matches DB count (dems_is_transferred = true)
    
    Actions:
    1. Update ALL evidence_files for this job_id with import success info
    2. Update evidence_transfer_jobs with completion status
    3. Send message to q-axon-transfer-completion queue
    """
    try:
        # Extract completedUtc from response
        completed_utc = response_data.get("completedUtc")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="handle_import_complete_success_start",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
                "completed_utc": completed_utc,
            },
        )
        
        # Get all evidence files for this job to update them
        evidence_files = db_manager.get_evidence_files_by_job(job_id)
        
        if not evidence_files:
            raise ValueError(f"No evidence files found for job_id: {job_id}")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="evidence_files_retrieved_for_update",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "evidence_file_count": len(evidence_files),
            },
        )
        
        # Prepare bulk update for evidence_files with import info
        # Using StatusCodes.IMPORTED (82) for state_code
        evidence_updates = []
        for ef in evidence_files:
            evidence_updates.append((
                ef['evidence_id'],
                StatusCodes.IMPORTED,  # 82
                dems_import_job_id,
                completed_utc
            ))
        
        # Bulk update all evidence files with import information
        update_result = db_manager.bulk_update_evidence_files_imported(
            evidence_updates=evidence_updates,
            last_modified_process="lambda: dems import status poller"
        )
        
        if not update_result.get("success"):
            raise Exception(f"Failed to update evidence files: {update_result.get('error')}")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="evidence_files_updated_successfully",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "updated_count": update_result.get("updated_count"),
            },
        )
        
        # Update evidence_transfer_jobs with IMPORTED status
        job_update_result = db_manager.update_job_status(
            job_id=job_id,
            status_code=StatusCodes.IMPORTED,  # 82
            job_msg=f"DEMS import completed: {dems_import_job_id}",
            last_modified_process="lambda: dems import status poller"
        )
        
        if not job_update_result.get("success"):
            raise Exception(f"Failed to update job status: {job_update_result.get('error')}")
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="evidence_transfer_job_updated_successfully",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "status_code": StatusCodes.IMPORTED,
            },
        )
        
        # Send message to q-axon-transfer-completion.fifo
        completion_queue_url = ssm_parameters.get("transfer_completion_queue_url")
        
        message_body = {"job_id": job_id}
        message_params = {
            "QueueUrl": completion_queue_url,
            "MessageBody": json.dumps(message_body),
            "MessageGroupId": f"job-{job_id}",
            "MessageDeduplicationId": f"{job_id}-{int(time.time())}",
        }
        
        sqs_response = sqs_client.send_message(**message_params)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="completion_queue_message_sent",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "queue_url": completion_queue_url,
                "message_id": sqs_response.get("MessageId"),
            },
        )
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="handle_import_complete_success_completed",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
            },
        )
        
        return {"status": "success", "path": "happy_path"}
        
    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="handle_import_complete_success_error",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        raise


def handle_import_complete_with_errors(
    response_data: Dict[str, Any],
    job_id: str,
    dems_case_id: str,
    dems_import_job_id: str,
    source_path: str,
    ssm_parameters: Dict[str, str],
    db_manager: Any,
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Handle import completion with errors/warnings (Alternate Path).
    
    Statuses: "Failed", "Deleted", "Rejected", "Cancelled", "Completed with errors",
              "Completed with warnings", "Completed with errors prematurely"
    OR: Status "Complete" but recordCount doesn't match DB count
    
    TODO: Implement:
    1. Evaluate how many files succeeded (recordCount)
    2. Collect fatal errors (null itemId in messages)
    3. Collect non-fatal errors (itemId present in messages)
    4. Correlate errors to evidence_file_name in DB
    5. Update evidence_files for each file:
        - Fatal error: state_code=83, dems_is_imported=0, error_msg="fatal error: {messageText}"
        - Non-fatal: state_code=84, dems_is_imported=1, error_msg="non-fatal error: {messageText}"
        - Success: state_code=82, dems_is_imported=1 (files not in error messages)
    6. Update evidence_transfer_jobs:
        - If fatal errors: state_code=83, job_msg="DEMS import failed with fatal errors"
        - If only non-fatal: state_code=84, job_msg="DEMS import completed with non-fatal errors"
    7. Send message to q-transfer-exception.fifo with job_id
    """
    logger.log(
        event=event,
        level=LogLevel.WARNING,
        status=LogStatus.SUCCESS,
        message="handle_import_complete_with_errors_placeholder",
        context_data={
            "env_stage": env_stage,
            "job_id": job_id,
            "dems_import_job_id": dems_import_job_id,
            "message": "TODO: Implement alternate path (errors) handler",
            "response_messages_count": len(response_data.get("messages", [])),
        },
    )
    
    # TODO: Implement alternate path logic
    return {"status": "success", "path": "alternate_path_with_errors"}


def handle_import_in_progress(
    job_id: str,
    dems_case_id: str,
    dems_import_job_id: str,
    source_path: str,
    import_name: str,
    attempt_number: int,
    ssm_parameters: Dict[str, str],
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Handle import still in progress or API call failure (Retry Path).
    
    Statuses: "Queued", "Importing", "Loading", "Deleting", "Rejecting", 
              "Processing", "Validating"
    OR: Import doesn't exist/invalid request
    Condition: attempt_number < max_retries
    
    TODO: Implement:
    1. Create new SQS message with incremented attempt_number
    2. Send to q-dems-import-status.fifo with:
        - job_id
        - dems_case_id
        - dems_import_job_id
        - source_path
        - attempt_number = attempt_number + 1
        - import_name (same as before)
    3. Set MessageDeduplicationId appropriately
    """
    next_attempt = attempt_number + 1
    retries_queue_url = ssm_parameters.get("import_status_retries_queue_url")
    if not retries_queue_url:
        raise ValueError("Missing SSM parameter: import_status_retries_queue_url")

    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="handle_import_in_progress_retry_enqueued",
        context_data={
            "env_stage": env_stage,
            "job_id": job_id,
            "dems_import_job_id": dems_import_job_id,
            "current_attempt": attempt_number,
            "next_attempt": next_attempt,
            "queue_url": retries_queue_url,
            "message": "Retrying import status check by re-queueing message",
        },
    )

    message_body = {
        "job_id": job_id,
        "dems_case_id": dems_case_id,
        "dems_import_job_id": dems_import_job_id,
        "source_path": source_path,
        "attempt_number": next_attempt,
        "import_name": import_name,
    }

    message_params = {
        "QueueUrl": retries_queue_url,
        "MessageBody": json.dumps(message_body),
        "MessageGroupId": f"job-{job_id}",
        "MessageDeduplicationId": f"{job_id}-{int(time.time())}",
    }

    sqs_response = sqs_client.send_message(**message_params)

    logger.log(
        event=event,
        level=LogLevel.INFO,
        status=LogStatus.SUCCESS,
        message="handle_import_in_progress_sqs_sent",
        context_data={
            "env_stage": env_stage,
            "job_id": job_id,
            "dems_import_job_id": dems_import_job_id,
            "message_id": sqs_response.get("MessageId"),
            "queue_url": retries_queue_url,
            "next_attempt": next_attempt,
        },
    )

    return {"status": "success", "path": "in_progress_retry", "message_id": sqs_response.get("MessageId")}


def handle_import_max_retries_reached(
    job_id: str,
    dems_case_id: str,
    dems_import_job_id: str,
    source_path: str,
    ssm_parameters: Dict[str, str],
    db_manager: Any,
    logger: LambdaStructuredLogger,
    event: Dict[str, Any],
    env_stage: str,
) -> Dict[str, Any]:
    """
    Handle max retries reached (Exception Path).
    
    Condition: Import still in progress OR doesn't exist, AND attempt_number >= max_retries
    
    TODO: Implement:
    1. Update evidence_transfer_jobs:
        - evidence_transfer_state_code = 83 (IMPORT-FAILED)
        - job_msg = "Import is failing for some reason; maximum time allowed reached"
        - last_modified_process = "lambda: dems import status poller"
        - last_modified_utc = UTC timestamp
    2. Update ALL evidence_files for this job_id:
        - evidence_transfer_state = 83 (IMPORT-FAILED)
        - dems_is_imported = 0 (false)
        - dems_imported_error_msg = "Import is failing for some reason; maximum time allowed reached"
        - last_modified_process = "lambda: dems import status poller"
        - last_modified_utc = UTC timestamp
    3. Send message to q-transfer-exception.fifo with job_id
    """
    logger.log(
        event=event,
        level=LogLevel.ERROR,
        status=LogStatus.FAILURE,
        message="handle_import_max_retries_reached_placeholder",
        context_data={
            "env_stage": env_stage,
            "job_id": job_id,
            "dems_import_job_id": dems_import_job_id,
            "message": "TODO: Implement exception path - max retries reached",
        },
    )
    
    # TODO: Implement exception path logic
    return {"status": "failure", "path": "max_retries_exceeded"}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """DEMS Import Status Poller Lambda function for processing SQS messages from q-dems-import-status.fifo.

    This function polls the DEMS API to check the status of import jobs and processes the results.

    Args:
        event: SQS event containing single message with:
            - job_id: Bridge tracking job ID
            - dems_case_id: DEMS case identifier
            - dems_import_job_id: DEMS import job identifier
            - source_path: Source path of the import
            - attempt_number: Current retry attempt number
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
        logger.log_start(event="dems_import_status_poller", job_id=request_id)

        # Extract SQS record
        records = event.get("Records", [])
        if not records:
            raise ValueError("No records found in SQS event")

        record: dict = records[0]
        message_id = record.get("messageId")
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="dems_import_status_poller_message_id",
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
            message="dems_import_status_poller_message_body",
            context_data={
                "env_stage": env_stage,
                "message_body": f"Message content: {message_body}",
            },
        )

        # Extract and validate message parameters
        job_id = message_body.get("job_id")
        dems_case_id = message_body.get("dems_case_id")
        dems_import_job_id = message_body.get("dems_import_job_id")
        source_path = message_body.get("source_path")
        attempt_number = message_body.get("attempt_number", 1)
        import_name = message_body.get("import_name")

        if not job_id:
            raise ValueError(f"Invalid Job ID: {job_id}")
        if not dems_case_id:
            raise ValueError(f"Invalid DEMS Case ID: {dems_case_id}")
        if not dems_import_job_id:
            raise ValueError(f"Invalid DEMS Import Job ID: {dems_import_job_id}")
        if not source_path:
            raise ValueError(f"Invalid Source Path: {source_path}")

        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="dems_import_status_poller_extracted_parameters",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "dems_case_id": dems_case_id,
                "dems_import_job_id": dems_import_job_id,
                "source_path": source_path,
                "attempt_number": attempt_number,
                "import_name": import_name,
            },
        )

        # Retrieve SSM parameters
        ssm_parameters = get_ssm_parameters(env_stage, logger, event, base_context)

        # Initialize database manager
        db_manager = get_db_manager(env_param_in=env_stage)

        # Get max retries from SSM parameter (required)
        max_retries_str = ssm_parameters.get("max_retries")
        if not max_retries_str:
            raise ValueError("Missing required SSM parameter: max_retries")
        max_retries = int(max_retries_str)
        
        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="dems_import_status_poller_parameters_ready",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "max_retries": max_retries,
                "attempt_number": attempt_number,
            },
        )

        # Call DEMS API to get import status
        try:
            response_data = call_dems_import_status_api(
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                base_url=ssm_parameters["import_status_url"],
                bearer_token=ssm_parameters["bearer"],
                logger=logger,
                event=event,
                env_stage=env_stage,
            )
        except Exception as e:
            logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="dems_import_status_api_call_failed",
                context_data={
                    "env_stage": env_stage,
                    "job_id": job_id,
                    "dems_import_job_id": dems_import_job_id,
                    "attempt_number": attempt_number,
                    "max_retries": max_retries,
                    "error": str(e),
                    "error_type": type(e).__name__,
                },
            )

            if attempt_number < max_retries:
                return handle_import_in_progress(
                    job_id=job_id,
                    dems_case_id=dems_case_id,
                    dems_import_job_id=dems_import_job_id,
                    source_path=source_path,
                    import_name=import_name,
                    attempt_number=attempt_number,
                    ssm_parameters=ssm_parameters,
                    logger=logger,
                    event=event,
                    env_stage=env_stage,
                )

            return handle_import_max_retries_reached(
                job_id=job_id,
                dems_case_id=dems_case_id,
                dems_import_job_id=dems_import_job_id,
                source_path=source_path,
                ssm_parameters=ssm_parameters,
                db_manager=db_manager,
                logger=logger,
                event=event,
                env_stage=env_stage,
            )

        # Evaluate response and handle accordingly
        result = evaluate_and_handle_import_status(
            response_data=response_data,
            job_id=job_id,
            dems_case_id=dems_case_id,
            dems_import_job_id=dems_import_job_id,
            source_path=source_path,
            import_name=import_name,
            attempt_number=attempt_number,
            max_retries=max_retries,
            ssm_parameters=ssm_parameters,
            db_manager=db_manager,
            logger=logger,
            event=event,
            env_stage=env_stage,
        )

        logger.log(
            event=event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message="dems_import_status_poller_completed",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "message_id": message_id,
                "result_path": result.get("path"),
                "result_status": result.get("status"),
            },
        )

        # Log the end of the function
        logger.log_end(event="dems_import_status_poller", job_id=request_id)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "DEMS import status polling completed",
                "job_id": job_id,
                "dems_import_job_id": dems_import_job_id,
                "path": result.get("path"),
                "status": result.get("status"),
            }),
        }

    except Exception as e:
        logger.log(
            event=event,
            level=LogLevel.ERROR,
            status=LogStatus.FAILURE,
            message="dems_import_status_poller_error",
            context_data={
                "env_stage": env_stage,
                "job_id": job_id,
                "message_id": message_id,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )

        # Log the end of the function with error
        logger.log_end(event="dems_import_status_poller", job_id=request_id)

        raise
