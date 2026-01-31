from typing import Dict, Any, Optional

import boto3
from lambda_structured_logger import LogLevel, LogStatus

from app.s3_stream_zipper import S3StreamZipper


class TransferPackageManager:
    """Handles S3 uploads and packaging (zipping) for transfer artifacts."""

    def __init__(self, logger, env_stage: str):
        self.logger = logger
        self.env_stage = env_stage
        self.s3_client = boto3.client('s3', region_name='ca-central-1')

    def _get_bucket(self, ssm_parameters: Dict[str, Any]) -> Optional[str]:
        """Retrieve S3 bucket from SSM parameters supporting both keys."""
        return ssm_parameters.get('s3_bucket') or ssm_parameters.get('bridge_s3_bucket')

    def build_folder_name(self, source_case_title: str, dems_case_id: str | None, job_id: str) -> str:
        """Use same convention as downloader: <source_case_title>_<dems_case_id>_<job_id>."""
        dems_part = dems_case_id or 'unknown'
        return f"{source_case_title}_{dems_part}_{job_id}"

    def create_and_upload_zip_stream(
        self,
        ssm_parameters: Dict[str, Any],
        case_info: Dict[str, Any],
        job_id: str,
        csv_filename: str,
        csv_filepath: str,
        event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        List all evidence files from S3 folder, stream them + local CSV into a zip, upload zip to S3.
        
        Args:
            ssm_parameters: SSM parameters including S3 bucket
            case_info: Case info with source_case_title and dems_case_id
            job_id: Job ID
            csv_filename: CSV filename
            csv_filepath: Local path to CSV
            event: Lambda event for logging
        
        Returns:
            Dict with success status and zip location
        """
        result = {
            'success': False,
            'zip_s3_uri': None,
            'error': None,
        }

        try:
            bucket = self._get_bucket(ssm_parameters)
            if not bucket:
                raise ValueError('S3 bucket missing in SSM parameters')

            # Build folder and zip key names
            source_case_title = case_info.get('source_case_title')
            dems_case_id = case_info.get('dems_case_id')
            folder_name = self.build_folder_name(source_case_title, dems_case_id, job_id)
            zip_key = f"{folder_name}.zip"

            self.logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="transfer_package_manager_stream_zip_start",
                context_data={
                    'env_stage': self.env_stage,
                    'bucket': bucket,
                    'folder_name': folder_name,
                    'zip_key': zip_key,
                },
            )

            # List all S3 objects in the folder (evidence files)
            zipper = S3StreamZipper(logger=self.logger, env_stage=self.env_stage)
            s3_objects = zipper.list_s3_objects(bucket, folder_name)

            self.logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="transfer_package_manager_objects_listed",
                context_data={
                    'env_stage': self.env_stage,
                    'object_count': len(s3_objects),
                },
            )

            # Stream zip to S3
            zip_result = zipper.stream_zip_to_s3(
                bucket=bucket,
                zip_key=zip_key,
                s3_objects=s3_objects,
                local_csv_path=csv_filepath,
                csv_filename=csv_filename,
                event=event,
            )

            if not zip_result.get('success'):
                raise Exception(f"Stream zip failed: {zip_result.get('error')}")

            result['success'] = True
            result['zip_s3_uri'] = zip_result.get('s3_uri')

            self.logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="transfer_package_manager_stream_zip_success",
                context_data={
                    'env_stage': self.env_stage,
                    'zip_s3_uri': result['zip_s3_uri'],
                },
            )

            return result

        except Exception as e:
            result['error'] = str(e)
            self.logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="transfer_package_manager_stream_zip_error",
                context_data={
                    'env_stage': self.env_stage,
                    'error': str(e),
                },
            )
            return result
