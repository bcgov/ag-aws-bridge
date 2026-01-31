import zipfile
import io
from typing import Dict, Any, List, Optional

import boto3
import zipstream
from lambda_structured_logger import LogLevel, LogStatus


class S3StreamZipper:
    """Streams S3 objects and local files directly into a zip without buffering to disk."""

    def __init__(self, logger, env_stage: str):
        self.logger = logger
        self.env_stage = env_stage
        self.s3_client = boto3.client('s3', region_name='ca-central-1')

    def list_s3_objects(self, bucket: str, prefix: str) -> List[Dict[str, Any]]:
        """List all objects under a given S3 prefix, excluding folder markers."""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            
            objects = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Skip folder markers (keys ending with '/') and zero-byte objects
                        # that are just directory placeholders
                        if not obj['Key'].endswith('/') and obj.get('Size', 0) > 0:
                            objects.append(obj)
            
            return objects
        except Exception as e:
            self.logger.log(
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="s3_stream_zipper_list_error",
                context_data={
                    'env_stage': self.env_stage,
                    'bucket': bucket,
                    'prefix': prefix,
                    'error': str(e),
                },
            )
            raise

    def stream_zip_to_s3(
        self,
        bucket: str,
        zip_key: str,
        s3_objects: List[Dict[str, Any]],
        local_csv_path: str,
        csv_filename: str,
        event: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Stream S3 objects and a local CSV into a zip, upload to S3 via multipart upload.
        
        Args:
            bucket: S3 bucket name
            zip_key: S3 key where the zip will be stored (e.g., 'folder_name.zip')
            s3_objects: List of S3 objects to include (from list_s3_objects)
            local_csv_path: Local path to the CSV file to include
            csv_filename: Filename for the CSV inside the zip
            event: Lambda event for logging
        
        Returns:
            Dict with success status and zip S3 location
        """
        result = {
            'success': False,
            's3_uri': None,
            'error': None,
        }

        try:
            self.logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="s3_stream_zipper_zip_start",
                context_data={
                    'env_stage': self.env_stage,
                    'bucket': bucket,
                    'zip_key': zip_key,
                    'object_count': len(s3_objects),
                },
            )

            # Create streaming zip file
            zf = zipstream.ZipFile(mode='w', compression=zipfile.ZIP_DEFLATED)

            # Add S3 objects to zip stream (exclude the zip file itself to avoid recursion)
            for obj in s3_objects:
                s3_key = obj['Key']
                
                # Skip the zip file we're creating (avoid including itself)
                if s3_key == zip_key:
                    continue
                
                # Use just the filename (last part of the key) to place files at zip root
                arcname = s3_key.split('/')[-1]
                
                try:
                    s3_response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
                    # Add S3 object as a stream to the zip
                    zf.write_iter(arcname=arcname, iterable=s3_response['Body'])
                except Exception as obj_error:
                    self.logger.log(
                        event=event,
                        level=LogLevel.WARNING,
                        status=LogStatus.SUCCESS,
                        message="s3_stream_zipper_skip_object",
                        context_data={
                            'env_stage': self.env_stage,
                            's3_key': s3_key,
                            'error': str(obj_error),
                        },
                    )
                    # Continue with next object

            # Add local CSV file to zip stream
            # Read CSV into memory and wrap in BytesIO for streaming
            try:
                with open(local_csv_path, 'rb') as csv_file:
                    csv_content = csv_file.read()
                # Wrap bytes in BytesIO to make it file-like
                csv_stream = io.BytesIO(csv_content)
                # Add CSV stream to zip
                zf.write_iter(arcname=csv_filename, iterable=csv_stream)
            except Exception as csv_error:
                raise ValueError(f"Failed to add CSV to zip: {str(csv_error)}")

            # Buffer all zip content into memory first
            # This is necessary because:
            # 1. Small files (< 5MB) cannot use multipart upload
            # 2. We need to know total size to choose upload method
            zip_buffer = io.BytesIO()
            for chunk in zf:
                zip_buffer.write(chunk)
            
            # Get size BEFORE seeking (tell() returns current position, which is at end after writing)
            zip_size = zip_buffer.tell()
            # Now seek back to beginning for reading
            zip_buffer.seek(0)

            # Use simple put_object for small files (< 5MB minimum for multipart)
            # Use multipart upload for larger files (more efficient for large data)
            if zip_size < 5 * 1024 * 1024:  # 5MB threshold
                # Simple upload for small files
                self.s3_client.put_object(
                    Bucket=bucket,
                    Key=zip_key,
                    Body=zip_buffer.getvalue(),
                )
                
                self.logger.log(
                    event=event,
                    level=LogLevel.INFO,
                    status=LogStatus.SUCCESS,
                    message="s3_stream_zipper_simple_upload",
                    context_data={
                        'env_stage': self.env_stage,
                        'zip_size_bytes': zip_size,
                    },
                )
            else:
                # Multipart upload for larger files
                multipart = self.s3_client.create_multipart_upload(Bucket=bucket, Key=zip_key)
                upload_id = multipart['UploadId']

                try:
                    parts = []
                    part_num = 1
                    chunk_size = 10 * 1024 * 1024  # 10MB chunks

                    while True:
                        chunk = zip_buffer.read(chunk_size)
                        if not chunk:
                            break
                        
                        part_response = self.s3_client.upload_part(
                            Bucket=bucket,
                            Key=zip_key,
                            PartNumber=part_num,
                            UploadId=upload_id,
                            Body=chunk,
                        )
                        parts.append({
                            'PartNumber': part_num,
                            'ETag': part_response['ETag'],
                        })
                        part_num += 1

                    # Complete multipart upload
                    self.s3_client.complete_multipart_upload(
                        Bucket=bucket,
                        Key=zip_key,
                        UploadId=upload_id,
                        MultipartUpload={'Parts': parts},
                    )
                    
                    self.logger.log(
                        event=event,
                        level=LogLevel.INFO,
                        status=LogStatus.SUCCESS,
                        message="s3_stream_zipper_multipart_upload",
                        context_data={
                            'env_stage': self.env_stage,
                            'zip_size_bytes': zip_size,
                            'parts_count': len(parts),
                        },
                    )

                except Exception as upload_error:
                    # Abort multipart upload on error
                    try:
                        self.s3_client.abort_multipart_upload(
                            Bucket=bucket,
                            Key=zip_key,
                            UploadId=upload_id,
                        )
                    except Exception as abort_error:
                        self.logger.log(
                            event=event,
                            level=LogLevel.WARNING,
                            status=LogStatus.SUCCESS,
                            message="s3_stream_zipper_abort_error",
                            context_data={
                                'env_stage': self.env_stage,
                                'error': str(abort_error),
                            },
                        )
                    raise

            # Verify upload
            self.s3_client.head_object(Bucket=bucket, Key=zip_key)

            result['success'] = True
            result['s3_uri'] = f"s3://{bucket}/{zip_key}"

            self.logger.log(
                event=event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message="s3_stream_zipper_zip_success",
                context_data={
                    'env_stage': self.env_stage,
                    's3_uri': result['s3_uri'],
                    'zip_size_bytes': zip_size,
                },
            )

            return result

        except Exception as e:
            result['error'] = str(e)
            self.logger.log(
                event=event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message="s3_stream_zipper_error",
                context_data={
                    'env_stage': self.env_stage,
                    'error': str(e),
                },
            )
            return result
