# ==================================================
# FILE: python/lambda_structured_logger/logger.py
# ==================================================

import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, Union

from .enums import LogLevel, LogStatus

class LambdaStructuredLogger:
    """
    Structured logger for AWS Lambda functions
    Provides consistent, structured JSON logging with automatic context
    gathering and CloudWatch integration.
    """
    
    def __init__(self):
        """Initialize the logger with Lambda context information"""
        self.lambda_function = os.getenv("AWS_LAMBDA_FUNCTION_NAME", "unknown_lambda")
        self.env_stage = os.getenv("ENV_STAGE", "unknown_env")
        self.aws_request_id = os.getenv("_X_AMZN_TRACE_ID", "unknown_request")
        self.lambda_version = os.getenv("AWS_LAMBDA_FUNCTION_VERSION", "unknown_version")
        
        # Configure Python logging for CloudWatch integration
        logging.basicConfig(
            level=logging.INFO,
            format='%(message)s',  # We handle formatting in structured logs
            force=True
        )
        self.logger = logging.getLogger()
    
    def log(
        self,
        *,
        event: str,
        status: Union[str, LogStatus],
        message: str,
        level: Union[str, LogLevel] = LogLevel.INFO,
        job_id: Optional[str] = None,
        axon_case_metadata: Optional[Dict[str, Any]] = None,
        axon_evidence_metadata: Optional[Dict[str, Any]] = None,
        api_metadata: Optional[Dict[str, Any]] = None,
        retry_context: Optional[Dict[str, Any]] = None,
        error_details: Optional[Dict[str, Any]] = None,
        performance_metrics: Optional[Dict[str, Any]] = None,
        custom_metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log a structured message
        
        Args:
            event: The operation or event being logged
            status: Success/failure status of the operation
            message: Human-readable description
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            job_id: Unique identifier for the workflow
            case_metadata: Metadata about cases (source_case_id, title, evidence counts, etc.)
            evidence_metadata: Metadata about evidence files (evidence_id, file_id, type, size, checksum, etc.)
            api_metadata: Information about API calls (request_url, method, status_code, response_time, etc.)
            retry_context: Retry attempt information
            error_details: Exception and error information
            performance_metrics: Timing and performance data
            custom_metadata: Any additional structured data
        """
        # Convert enums to strings if needed
        if isinstance(status, LogStatus):
            status = status.value
        if isinstance(level, LogLevel):
            level = level.value
            
        # Build the base log entry
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "env_stage": self.env_stage,
            "lambda_function": self.lambda_function,
            "lambda_version": self.lambda_version,
            "aws_request_id": self.aws_request_id,
            "event": event,
            "status": status,
            "message": message
        }
        
        # Add optional fields only if they have values
        optional_fields = {
            "job_id": job_id,
            "case_metadata": case_metadata,
            "evidence_metadata": evidence_metadata,
            "api_metadata": api_metadata,
            "retry_context": retry_context,
            "error_details": error_details,
            "performance_metrics": performance_metrics,
            "custom_metadata": custom_metadata
        }
        
        for key, value in optional_fields.items():
            if value is not None:
                log_entry[key] = value
        
        # Output to appropriate logging level
        log_message = json.dumps(log_entry, default=str)  # default=str handles datetime objects
        
        if level == LogLevel.DEBUG.value:
            self.logger.debug(log_message)
        elif level == LogLevel.INFO.value:
            self.logger.info(log_message)
        elif level == LogLevel.WARNING.value:
            self.logger.warning(log_message)
        elif level == LogLevel.ERROR.value:
            self.logger.error(log_message)
        elif level == LogLevel.CRITICAL.value:
            self.logger.critical(log_message)
        else:
            self.logger.info(log_message)
    
    def log_start(self, event: str, job_id: Optional[str] = None, **kwargs):
        """Log the start of a process"""
        self.log(
            event=event,
            status=LogStatus.IN_PROGRESS,
            message=f"Starting {event}",
            job_id=job_id,
            **kwargs
        )
    
    def log_success(self, event: str, message: str, job_id: Optional[str] = None, **kwargs):
        """Log successful completion"""
        self.log(
            event=event,
            status=LogStatus.SUCCESS,
            message=message,
            job_id=job_id,
            **kwargs
        )
    
    def log_error(self, event: str, error: Exception, job_id: Optional[str] = None, **kwargs):
        """Log an error with exception details"""
        import traceback
        
        error_details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_traceback": traceback.format_exc()
        }
        
        self.log(
            event=event,
            status=LogStatus.FAILURE,
            message=f"Error in {event}: {str(error)}",
            level=LogLevel.ERROR,
            job_id=job_id,
            error_details=error_details,
            **kwargs
        )
    
    def log_api_call(self, event: str, url: str, method: str, status_code: int, 
                     response_time: float, job_id: Optional[str] = None, **kwargs):
        """Log API (Axon and ISL) call with standardized metadata"""
        api_metadata = {
            "request_url": url,
            "request_method": method,
            "response_http_status": status_code,
            "response_time_ms": response_time
        }
        
        status = LogStatus.SUCCESS if 200 <= status_code < 300 else LogStatus.FAILURE
        level = LogLevel.INFO if status == LogStatus.SUCCESS else LogLevel.ERROR
        
        self.log(
            event=event,
            status=status,
            message=f"API {method} {url} returned {status_code}",
            level=level,
            job_id=job_id,
            api_metadata=api_metadata,
            **kwargs
        )

    def log_ssm_parameter_collection(self, parameter_names: list, parameters_collected: Dict[str, str],
                                   response_time_ms: float, invalid_parameters: list = None,
                                   job_id: Optional[str] = None, **kwargs):
        """
        Log SSM parameter collection results (after Lambda function collected them)
        
        Args:
            parameter_names: List of parameter names that were requested
            parameters_collected: Dictionary of successfully collected parameters
            response_time_ms: Time taken to collect parameters
            invalid_parameters: List of parameters that failed to collect
            job_id: Unique identifier for the workflow
        """
        invalid_parameters = invalid_parameters or []
        
        self.log_success(
            event="ssm_parameter_collection",
            message=f"Successfully collected {len(parameters_collected)} SSM parameters",
            job_id=job_id,
            performance_metrics={
                "response_time_ms": response_time_ms,
                "parameters_requested": len(parameter_names),
                "parameters_collected": len(parameters_collected),
                "parameters_failed": len(invalid_parameters)
            },
            custom_metadata={
                "parameter_names": parameter_names,
                "invalid_parameters": invalid_parameters,
                "collected_parameter_keys": list(parameters_collected.keys())
            }
        )
        
        # Log warning if some parameters failed
        if invalid_parameters:
            self.log(
                event="ssm_parameter_collection",
                status=LogStatus.SUCCESS,
                message=f"Some parameters not found: {invalid_parameters}",
                level=LogLevel.WARNING,
                job_id=job_id,
                custom_metadata={"missing_parameters": invalid_parameters}
            )
            
    def log_ssm_secure_string_store(self, parameter_name: str, response_time_ms: float,
                               parameter_version: Optional[int] = None,
                               overwrite: bool = False, job_id: Optional[str] = None, **kwargs):
    """
    Log SSM SecureString parameter storage results (after Authentication function stored the token)
    
    Args:
        parameter_name: SSM parameter name where the secure string was stored
        response_time_ms: Time taken to store the parameter
        parameter_version: Version number returned by SSM (if available)
        overwrite: Whether an existing parameter was overwritten
        job_id: Unique identifier for the workflow
    """
    self.log_success(
        event="ssm_secure_string_store",
        message=f"Bearer token stored in SSM parameter {parameter_name}",
        job_id=job_id,
        performance_metrics={
            "response_time_ms": response_time_ms,
            "parameter_version": parameter_version
        },
        custom_metadata={
            "parameter_name": parameter_name,
            "parameter_type": "SecureString",
            "operation": "overwrite" if overwrite else "create",
            "token_type": "bearer_token"
        }
    )
    
    def log_database_update_jobs(self, job_id: str, status: str, rows_affected: int,
                               response_time_ms: float, evidence_metadata: Optional[Dict] = None,
                               **kwargs):
        """
        Log evidence_transfer_jobs table update results (after Lambda function updated it)
        
        Args:
            job_id: The job ID that was updated
            status: New status that was set
            rows_affected: Number of rows affected by the update
            response_time_ms: Time taken for the database operation
            evidence_metadata: Metadata that was stored (if any)
        """
        self.log_success(
            event="database_update_jobs",
            message=f"Updated evidence_transfer_jobs for job {job_id}",
            job_id=job_id,
            performance_metrics={
                "response_time_ms": response_time_ms,
                "rows_affected": rows_affected
            },
            custom_metadata={
                "table": "evidence_transfer_jobs",
                "operation": "update",
                "new_status": status,
                "metadata_included": evidence_metadata is not None
            }
        )
        
        if rows_affected == 0:
            self.log(
                event="database_update_jobs",
                status=LogStatus.SUCCESS,
                message=f"No rows updated for job {job_id} - job may not exist",
                level=LogLevel.WARNING,
                job_id=job_id
            )
    
    def log_database_update_files(self, evidence_id: str, columns_updated: list, rows_affected: int,
                                response_time_ms: float, updated_values: Optional[Dict] = None,
                                job_id: Optional[str] = None, **kwargs):
        """
        Log evidence_files table update results (after Lambda function updated it)
        
        Args:
            evidence_id: The evidence ID that was updated
            columns_updated: List of column names that were updated
            rows_affected: Number of rows affected by the update
            response_time_ms: Time taken for the database operation
            updated_values: Dictionary of the values that were updated
            job_id: Unique identifier for the workflow
        """
        self.log_success(
            event="database_update_files",
            message=f"Updated evidence_files for evidence {evidence_id}",
            job_id=job_id,
            performance_metrics={
                "response_time_ms": response_time_ms,
                "rows_affected": rows_affected
            },
            custom_metadata={
                "table": "evidence_files",
                "operation": "update",
                "columns_updated": columns_updated,
                "evidence_id": evidence_id
            },
            evidence_metadata={
                "evidence_id": evidence_id,
                **{k: v for k, v in (updated_values or {}).items() if k in [
                    'evidence_file_id', 'evidence_file_type', 
                    'evidence_file_size', 'evidence_file_checksum'
                ]}
            }
        )
        
        if rows_affected == 0:
            self.log(
                event="database_update_files",
                status=LogStatus.SUCCESS,
                message=f"No rows updated for evidence {evidence_id} - evidence may not exist",
                level=LogLevel.WARNING,
                job_id=job_id
            )
    
    def log_sqs_message_sent(self, queue_url: str, message_id: str, message_body: Dict[str, Any],
                           response_time_ms: float, message_attributes: Optional[Dict] = None,
                           job_id: Optional[str] = None, **kwargs):
        """
        Log SQS message sending results (after Lambda function sent the message)
        
        Args:
            queue_url: SQS queue URL where message was sent
            message_id: SQS Message ID returned from send operation
            message_body: The message body that was sent
            response_time_ms: Time taken to send the message
            message_attributes: SQS message attributes that were sent
            job_id: Unique identifier for the workflow
        """
        import json
        
        message_body_str = json.dumps(message_body)
        
        self.log_success(
            event="sqs_message_send",
            message=f"SQS message sent successfully",
            job_id=job_id,
            performance_metrics={
                "response_time_ms": response_time_ms,
                "message_size_bytes": len(message_body_str)
            },
            custom_metadata={
                "queue_url": queue_url,
                "message_id": message_id,
                "message_attributes_count": len(message_attributes or {}),
                "next_step": message_body.get('step', 'unknown')
            }
        )
    
    def log_case_metadata(self, source_case_id: str, source_case_title: str,
                         source_case_evidence_count_total: int,
                         source_case_evidence_count_to_download: int,
                         source_case_evidence_count_downloaded: int,
                         event: str = "case_metadata_processed",
                         status: Union[str, LogStatus] = LogStatus.SUCCESS,
                         message: Optional[str] = None,
                         job_id: Optional[str] = None, **kwargs):
        """
        Log standardized case metadata information
        
        Args:
            source_case_id: Unique identifier for the case in source system
            source_case_title: Human-readable case title
            source_case_evidence_count_total: Total evidence files in the case
            source_case_evidence_count_to_download: Number of files to download
            source_case_evidence_count_downloaded: Number of files already downloaded
            event: Event name for this log entry
            status: Status of the operation
            message: Custom message (auto-generated if not provided)
            job_id: Unique identifier for the workflow
        """
        if not message:
            progress_pct = (source_case_evidence_count_downloaded / 
                          source_case_evidence_count_total * 100) if source_case_evidence_count_total > 0 else 0
            message = (f"Case {source_case_id}: {source_case_evidence_count_downloaded}/"
                      f"{source_case_evidence_count_total} evidence files downloaded ({progress_pct:.1f}%)")
        
        case_metadata = {
            "source_case_id": source_case_id,
            "source_case_title": source_case_title,
            "source_case_evidence_count_total": source_case_evidence_count_total,
            "source_case_evidence_count_to_download": source_case_evidence_count_to_download,
            "source_case_evidence_count_downloaded": source_case_evidence_count_downloaded
        }
        
        self.log(
            event=event,
            status=status,
            message=message,
            job_id=job_id,
            case_metadata=case_metadata,
            custom_metadata={
                "progress_percentage": (source_case_evidence_count_downloaded / 
                                      source_case_evidence_count_total * 100) if source_case_evidence_count_total > 0 else 0,
                "remaining_downloads": source_case_evidence_count_to_download - source_case_evidence_count_downloaded
            },
            **kwargs
        )
    
    def log_evidence_metadata(self, evidence_id: str, evidence_file_id: Optional[str] = None,
                            evidence_file_type: Optional[str] = None,
                            evidence_file_size: Optional[int] = None,
                            evidence_file_checksum: Optional[str] = None,
                            event: str = "evidence_metadata_processed",
                            status: Union[str, LogStatus] = LogStatus.SUCCESS,
                            message: Optional[str] = None,
                            job_id: Optional[str] = None, **kwargs):
        """
        Log standardized evidence metadata information
        
        Args:
            evidence_id: Unique identifier for the evidence
            evidence_file_id: File identifier in the source system
            evidence_file_type: Type/format of the evidence file
            evidence_file_size: Size of the file in bytes
            evidence_file_checksum: Checksum/hash of the file
            event: Event name for this log entry
            status: Status of the operation
            message: Custom message (auto-generated if not provided)
            job_id: Unique identifier for the workflow
        """
        if not message:
            file_size_mb = evidence_file_size / (1024 * 1024) if evidence_file_size else 0
            message = (f"Evidence {evidence_id} ({evidence_file_type or 'unknown type'}, "
                      f"{file_size_mb:.2f}MB) metadata processed")
        
        evidence_metadata = {
            "evidence_id": evidence_id
        }
        
        # Only include non-None values
        if evidence_file_id is not None:
            evidence_metadata["evidence_file_id"] = evidence_file_id
        if evidence_file_type is not None:
            evidence_metadata["evidence_file_type"] = evidence_file_type
        if evidence_file_size is not None:
            evidence_metadata["evidence_file_size"] = evidence_file_size
        if evidence_file_checksum is not None:
            evidence_metadata["evidence_file_checksum"] = evidence_file_checksum
        
        # Calculate additional metrics
        custom_metadata = {}
        if evidence_file_size:
            custom_metadata["file_size_mb"] = round(evidence_file_size / (1024 * 1024), 2)
            custom_metadata["file_size_category"] = self._categorize_file_size(evidence_file_size)
        
        if evidence_file_checksum:
            custom_metadata["checksum_algorithm"] = self._detect_checksum_algorithm(evidence_file_checksum)
        
        self.log(
            event=event,
            status=status,
            message=message,
            job_id=job_id,
            evidence_metadata=evidence_metadata,
            custom_metadata=custom_metadata,
            **kwargs
        )
    
    def log_case_progress(self, source_case_id: str, current_downloaded: int, 
                         total_evidence: int, job_id: Optional[str] = None, **kwargs):
        """
        Log case download progress with automatic progress calculations
        
        Args:
            source_case_id: Unique identifier for the case
            current_downloaded: Current number of files downloaded
            total_evidence: Total number of evidence files in case
            job_id: Unique identifier for the workflow
        """
        progress_pct = (current_downloaded / total_evidence * 100) if total_evidence > 0 else 0
        remaining = total_evidence - current_downloaded
        
        # Determine status based on progress
        if progress_pct == 100:
            status = LogStatus.SUCCESS
            message = f"Case {source_case_id} download completed"
        elif progress_pct > 0:
            status = LogStatus.IN_PROGRESS
            message = f"Case {source_case_id} download progress: {current_downloaded}/{total_evidence} files"
        else:
            status = LogStatus.IN_PROGRESS
            message = f"Case {source_case_id} download starting"
        
        self.log(
            event="case_download_progress",
            status=status,
            message=message,
            job_id=job_id,
            case_metadata={
                "source_case_id": source_case_id,
                "source_case_evidence_count_total": total_evidence,
                "source_case_evidence_count_downloaded": current_downloaded
            },
            performance_metrics={
                "progress_percentage": round(progress_pct, 2),
                "files_remaining": remaining
            },
            **kwargs
        )
    
    def log_evidence_processing(self, evidence_id: str, processing_step: str,
                              file_size: Optional[int] = None,
                              processing_time_ms: Optional[float] = None,
                              job_id: Optional[str] = None, **kwargs):
        """
        Log evidence processing steps with performance metrics
        
        Args:
            evidence_id: Unique identifier for the evidence
            processing_step: Name of the processing step
            file_size: Size of the file being processed
            processing_time_ms: Time taken for processing in milliseconds
            job_id: Unique identifier for the workflow
        """
        performance_metrics = {}
        if processing_time_ms:
            performance_metrics["processing_time_ms"] = processing_time_ms
        if file_size:
            performance_metrics["file_size_bytes"] = file_size
            performance_metrics["file_size_mb"] = round(file_size / (1024 * 1024), 2)
            if processing_time_ms:
                # Calculate processing speed in MB/s
                speed_mbps = (file_size / (1024 * 1024)) / (processing_time_ms / 1000)
                performance_metrics["processing_speed_mbps"] = round(speed_mbps, 2)
        
        self.log(
            event=f"evidence_{processing_step}",
            status=LogStatus.SUCCESS,
            message=f"Evidence {evidence_id} {processing_step} completed",
            job_id=job_id,
            evidence_metadata={"evidence_id": evidence_id},
            performance_metrics=performance_metrics,
            **kwargs
        )
    
    def _categorize_file_size(self, size_bytes: int) -> str:
        """Categorize file size for analytics"""
        if size_bytes < 1024 * 1024:  # < 1MB
            return "small"
        elif size_bytes < 100 * 1024 * 1024:  # < 100MB
            return "medium"
        elif size_bytes < 1024 * 1024 * 1024:  # < 1GB
            return "large"
        else:
            return "extra_large"
    
    def _detect_checksum_algorithm(self, checksum: str) -> str:
        """Detect checksum algorithm based on length"""
        length = len(checksum)
        if length == 32:
            return "md5"
        elif length == 40:
            return "sha1"
        elif length == 64:
            return "sha256"
        elif length == 128:
            return "sha512"
        else:
            return "unknown"
