# python/bridge_tracking_db_layer.py - Lambda Layer Database Module
import os
import json
import logging
import boto3
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DatabaseManager:
    """Database manager with connection pooling for evidence transfer system."""
    
    def __init__(self):
        self.connection_pool = None
        self.ssm_client = None
        self._db_config = None
        self._queue_config = None
        self._initialize_pool()
    
    def _get_ssm_client(self):
        """Get or create SSM client."""
        if self.ssm_client is None:
            self.ssm_client = boto3.client('ssm')
        return self.ssm_client
    
    def _get_db_config_from_ssm(self):
        """Retrieve database configuration from AWS SSM Parameter Store."""
        if self._db_config is not None:
            return self._db_config
        
        try:
            ssm = self._get_ssm_client()
            
        # Get environment stage from environment variable
        env_stage = os.environ.get('ENV_STAGE', 'dev-test')
        logger.info(f"Loading configuration for environment: {env_stage}")
            
            # Define SSM parameter paths          
            parameter_paths = {
                'host': f'/{env_stage}/bridge/tracking-db/host',
                'name': f'/{env_stage}/bridge/tracking-db/name',
                'username': f'/{env_stage}/bridge/tracking-db/username',
                'password': f'/{env_stage}/bridge/tracking-db/password',
                'port': f'/{env_stage}/bridge/tracking-db/port'
        }
            
            # Get all parameters in a single call for efficiency
            parameter_names = list(parameter_paths.values())
            
            response = ssm.get_parameters(
                Names=parameter_names,
                WithDecryption=True  # Decrypt SecureString parameters
            )
            
            # Check if any parameters were not found
            if len(response['Parameters']) != len(parameter_names):
                missing_params = set(parameter_names) - {p['Name'] for p in response['Parameters']}
                raise ValueError(f"Missing SSM parameters: {missing_params}")
            
            # Build configuration dictionary
            config = {}
            for param in response['Parameters']:
                for key, path in parameter_paths.items():
                    if param['Name'] == path:
                        config[key] = param['Value']
                        break
            
            # Convert port to integer
            config['port'] = int(config['port'])
            
            self._db_config = config
            logger.info("Database configuration loaded from SSM Parameter Store")
            
            return self._db_config
            
        except Exception as e:
            logger.error(f"Failed to retrieve database configuration from SSM: {e}")
            raise
    
    def _initialize_pool(self):
        """Initialize the connection pool using SSM parameters."""
        if self.connection_pool is None:
            try:
                db_config = self._get_db_config_from_ssm()
                
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    1, 10,  # min and max connections
                    host=db_config['host'],
                    port=db_config['port'],
                    database=db_config['name'],
                    user=db_config['username'],
                    password=db_config['password'],
                    sslmode='require'  # Always use SSL for production
                )
                logger.info(f"Database connection pool initialized for host: {db_config['host']}")
            except Exception as e:
                logger.error(f"Failed to initialize connection pool: {e}")
                raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results as list of dictionaries."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                return []
    
    def execute_query_one(self, query: str, params: tuple = None) -> Optional[Dict]:
        """Execute a query and return first result as dictionary."""
        results = self.execute_query(query, params)
        return results[0] if results else None
    
    def execute_transaction(self, queries: List[Tuple[str, tuple]]) -> List[Dict]:
        """Execute multiple queries in a transaction."""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                results = []
                try:
                    # Start transaction (auto-started, but explicit for clarity)
                    cursor.execute("BEGIN")
                    
                    for query, params in queries:
                        cursor.execute(query, params)
                        if cursor.description:
                            results.append([dict(row) for row in cursor.fetchall()])
                        else:
                            results.append([])
                    
                    # Commit transaction
                    cursor.execute("COMMIT")
                    return results
                    
                except Exception as e:
                    # Rollback on any error
                    cursor.execute("ROLLBACK")
                    logger.error(f"Transaction failed, rolled back: {e}")
                    raise

    def create_evidence_files_atomic(self, files_data: List[Dict[str, Any]], job_id: str, 
                                   last_modified_process: str) -> Dict[str, Any]:
        """
        Atomically create multiple evidence files and update job counts.
        Returns summary of what was created.
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                try:
                    cursor.execute("BEGIN")
                    
                    created_files = []
                    skipped_files = []
                    
                    # Create evidence files, handling duplicates gracefully
                    for file_data in files_data:
                        try:
                            insert_query = """
                                INSERT INTO evidence_files (
                                    evidence_id, job_id, evidence_transfer_state_code, evidence_file_id,
                                    evidence_file_type, source_case_id, file_size_bytes, checksum,
                                    axon_is_downloaded, dems_is_transferred, dems_is_imported,
                                    last_modified_process, last_modified_utc, retry_count
                                ) VALUES (
                                    %(evidence_id)s, %(job_id)s, %(evidence_transfer_state_code)s, %(evidence_file_id)s,
                                    %(evidence_file_type)s, %(source_case_id)s, %(file_size_bytes)s, %(checksum)s,
                                    false, false, false, %(last_modified_process)s, NOW(), %(retry_count)s
                                ) RETURNING *
                            """
                            
                            file_data.setdefault('retry_count', 0)
                            cursor.execute(insert_query, file_data)
                            created_file = dict(cursor.fetchone())
                            created_files.append(created_file)
                            
                        except psycopg2.IntegrityError as e:
                            # Handle unique constraint violation (duplicate evidence_id)
                            if "duplicate key" in str(e).lower() or "unique" in str(e).lower():
                                logger.warning(f"Evidence file {file_data['evidence_id']} already exists, skipping")
                                skipped_files.append(file_data['evidence_id'])
                                # Continue with transaction - this is expected behavior
                            else:
                                # Other integrity errors should fail the transaction
                                raise
                    
                    # Update job counts if any files were created
                    if created_files:
                        # Count files by state
                        normal_count = len([f for f in created_files 
                                          if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY])
                        oversize_count = len([f for f in created_files 
                                            if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY_OVERSIZE])
                        total_new_files = normal_count + oversize_count
                        
                        # Get current job counts
                        cursor.execute("SELECT * FROM evidence_transfer_jobs WHERE job_id = %s", (job_id,))
                        current_job = dict(cursor.fetchone())
                        
                        # Calculate new counts
                        new_total = current_job['source_case_evidence_count_total'] + total_new_files
                        new_to_download = current_job['source_case_evidence_count_to_download'] + total_new_files
                        
                        # Update job counts
                        update_query = """
                            UPDATE evidence_transfer_jobs 
                            SET source_case_evidence_count_total = %s,
                                source_case_evidence_count_to_download = %s,
                                last_modified_process = %s, 
                                last_modified_utc = NOW()
                            WHERE job_id = %s 
                            RETURNING *
                        """
                        
                        cursor.execute(update_query, (new_total, new_to_download, last_modified_process, job_id))
                        updated_job = dict(cursor.fetchone())
                    
                    cursor.execute("COMMIT")
                    
                    return {
                        'success': True,
                        'created_files': created_files,
                        'skipped_files': skipped_files,
                        'created_count': len(created_files),
                        'skipped_count': len(skipped_files),
                        'normal_files_count': len([f for f in created_files 
                                                 if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY]),
                        'oversize_files_count': len([f for f in created_files 
                                                   if f['evidence_transfer_state_code'] == StatusCodes.DOWNLOAD_READY_OVERSIZE])
                    }
                    
                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logger.error(f"Atomic evidence file creation failed: {e}")
                    return {
                        'success': False,
                        'error': str(e),
                        'created_files': [],
                        'skipped_files': [],
                        'created_count': 0,
                        'skipped_count': 0
                    }

    def bulk_update_evidence_file_states(self, evidence_updates: List[Tuple[str, int]], 
                                        last_modified_process: str) -> Dict[str, Any]:
        """
        Atomically update multiple evidence file states.
        evidence_updates: List of (evidence_id, new_state_code) tuples
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                try:
                    cursor.execute("BEGIN")
                    
                    updated_files = []
                    
                    for evidence_id, new_state_code in evidence_updates:
                        update_query = """
                            UPDATE evidence_files 
                            SET evidence_transfer_state_code = %s, 
                                last_modified_process = %s, 
                                last_modified_utc = NOW()
                            WHERE evidence_id = %s 
                            RETURNING *
                        """
                        
                        cursor.execute(update_query, (new_state_code, last_modified_process, evidence_id))
                        if cursor.rowcount > 0:
                            updated_file = dict(cursor.fetchone())
                            updated_files.append(updated_file)
                    
                    cursor.execute("COMMIT")
                    
                    return {
                        'success': True,
                        'updated_files': updated_files,
                        'updated_count': len(updated_files)
                    }
                    
                except Exception as e:
                    cursor.execute("ROLLBACK")
                    logger.error(f"Bulk evidence file state update failed: {e}")
                    return {
                        'success': False,
                        'error': str(e),
                        'updated_files': [],
                        'updated_count': 0
                    }

    # Evidence Transfer Jobs methods
    def create_evidence_transfer_job(self, job_data: Dict[str, Any]) -> Dict:
        """Create a new evidence transfer job."""
        query = """
            INSERT INTO evidence_transfer_jobs (
                job_id, job_created_utc, job_status_code, job_msg, source_system, source_agency,
                source_case_id, source_case_title, source_case_last_modified_utc,
                source_case_evidence_count_total, source_case_evidence_count_to_download,
                source_case_evidence_count_downloaded, dems_case_id, last_modified_process,
                last_modified_utc, retry_count, max_retries
            ) VALUES (
                %(job_id)s, NOW(), %(job_status_code)s, %(job_msg)s, %(source_system)s, %(source_agency)s,
                %(source_case_id)s, %(source_case_title)s, %(source_case_last_modified_utc)s,
                %(source_case_evidence_count_total)s, %(source_case_evidence_count_to_download)s,
                %(source_case_evidence_count_downloaded)s, %(dems_case_id)s, %(last_modified_process)s,
                NOW(), %(retry_count)s, %(max_retries)s
            ) RETURNING *
        """
        
        # Set defaults
        job_data.setdefault('job_msg', None)
        job_data.setdefault('source_case_evidence_count_to_download', 0)
        job_data.setdefault('source_case_evidence_count_downloaded', 0)
        job_data.setdefault('dems_case_id', None)
        job_data.setdefault('retry_count', 0)
        job_data.setdefault('max_retries', 3)
        
        return self.execute_query_one(query, job_data)
    
    def get_evidence_transfer_job(self, job_id: str) -> Optional[Dict]:
        """Get an evidence transfer job by ID."""
        query = "SELECT * FROM evidence_transfer_jobs WHERE job_id = %s"
        return self.execute_query_one(query, (job_id,))
    
    def update_job_status(self, job_id: str, status_code: int, job_msg: str = None, 
                         last_modified_process: str = None) -> Dict:
        """Update job status."""
        query = """
            UPDATE evidence_transfer_jobs 
            SET job_status_code = %s, job_msg = %s, last_modified_process = %s, last_modified_utc = NOW()
            WHERE job_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (status_code, job_msg, last_modified_process, job_id))
    
    def update_job_counts(self, job_id: str, total: int = None, to_download: int = None, 
                         downloaded: int = None, last_modified_process: str = None) -> Dict:
        """Update job evidence counts."""
        updates = []
        params = []
        
        if total is not None:
            updates.append("source_case_evidence_count_total = %s")
            params.append(total)
        
        if to_download is not None:
            updates.append("source_case_evidence_count_to_download = %s")
            params.append(to_download)
        
        if downloaded is not None:
            updates.append("source_case_evidence_count_downloaded = %s")
            params.append(downloaded)
        
        updates.extend(["last_modified_process = %s", "last_modified_utc = NOW()"])
        params.extend([last_modified_process, job_id])
        
        query = f"""
            UPDATE evidence_transfer_jobs 
            SET {', '.join(updates)}
            WHERE job_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, tuple(params))
    
    def increment_job_retry_count(self, job_id: str, last_modified_process: str) -> Dict:
        """Increment job retry count."""
        query = """
            UPDATE evidence_transfer_jobs 
            SET retry_count = retry_count + 1, last_modified_process = %s, last_modified_utc = NOW()
            WHERE job_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (last_modified_process, job_id))
    
    def get_jobs_by_status(self, status_code: int, limit: int = 100) -> List[Dict]:
        """Get jobs by status code."""
        query = """
            SELECT * FROM evidence_transfer_jobs 
            WHERE job_status_code = %s 
            ORDER BY job_created_utc DESC 
            LIMIT %s
        """
        return self.execute_query(query, (status_code, limit))
    
    def set_dems_case(self, job_id: str, dems_case_id: str, last_modified_process: str) -> Dict:
        """Set DEMS case ID for a job."""
        query = """
            UPDATE evidence_transfer_jobs 
            SET dems_case_id = %s, last_modified_process = %s, last_modified_utc = NOW()
            WHERE job_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (dems_case_id, last_modified_process, job_id))

    # Evidence Files methods
    def create_evidence_file(self, file_data: Dict[str, Any]) -> Dict:
        """Create a new evidence file record."""
        query = """
            INSERT INTO evidence_files (
                evidence_id, job_id, evidence_transfer_state_code, evidence_file_id,
                evidence_file_type, source_case_id, file_size_bytes, checksum,
                axon_is_downloaded, dems_is_transferred, dems_is_imported,
                last_modified_process, last_modified_utc, retry_count
            ) VALUES (
                %(evidence_id)s, %(job_id)s, %(evidence_transfer_state_code)s, %(evidence_file_id)s,
                %(evidence_file_type)s, %(source_case_id)s, %(file_size_bytes)s, %(checksum)s,
                false, false, false, %(last_modified_process)s, NOW(), %(retry_count)s
            ) RETURNING *
        """
        
        file_data.setdefault('retry_count', 0)
        return self.execute_query_one(query, file_data)
    
    def update_evidence_file_state(self, evidence_id: str, state_code: int, 
                                  last_modified_process: str) -> Dict:
        """Update evidence file state."""
        query = """
            UPDATE evidence_files 
            SET evidence_transfer_state_code = %s, last_modified_process = %s, last_modified_utc = NOW()
            WHERE evidence_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (state_code, last_modified_process, evidence_id))
    
    def mark_file_downloaded(self, evidence_id: str, error_msg: str = None, 
                           last_modified_process: str = None) -> Dict:
        """Mark file as downloaded (or failed)."""
        is_downloaded = error_msg is None
        query = """
            UPDATE evidence_files 
            SET axon_is_downloaded = %s, axon_download_error_msg = %s, 
                axon_download_utc = NOW(), last_modified_process = %s, last_modified_utc = NOW()
            WHERE evidence_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (is_downloaded, error_msg, last_modified_process, evidence_id))
    
    def mark_file_transferred(self, evidence_id: str, error_msg: str = None, 
                            last_modified_process: str = None) -> Dict:
        """Mark file as transferred to DEMS (or failed)."""
        is_transferred = error_msg is None
        query = """
            UPDATE evidence_files 
            SET dems_is_transferred = %s, dems_transfer_error_msg = %s, 
                dems_transferred_utc = NOW(), last_modified_process = %s, last_modified_utc = NOW()
            WHERE evidence_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (is_transferred, error_msg, last_modified_process, evidence_id))
    
    def mark_file_imported(self, evidence_id: str, dems_imported_id: str = None, 
                          error_msg: str = None, last_modified_process: str = None) -> Dict:
        """Mark file as imported into DEMS (or failed)."""
        is_imported = error_msg is None
        query = """
            UPDATE evidence_files 
            SET dems_is_imported = %s, dems_imported_id = %s, dems_imported_error_msg = %s,
                dems_imported_utc = NOW(), last_modified_process = %s, last_modified_utc = NOW()
            WHERE evidence_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (is_imported, dems_imported_id, error_msg, 
                                            last_modified_process, evidence_id))
    
    def get_evidence_file(self, evidence_id: str) -> Optional[Dict]:
        """Get an evidence file by ID."""
        query = "SELECT * FROM evidence_files WHERE evidence_id = %s"
        return self.execute_query_one(query, (evidence_id,))
    
    def get_evidence_files_by_job(self, job_id: str) -> List[Dict]:
        """Get all evidence files for a job."""
        query = "SELECT * FROM evidence_files WHERE job_id = %s ORDER BY evidence_id"
        return self.execute_query(query, (job_id,))
    
    def get_evidence_files_by_state(self, state_code: int, limit: int = 100) -> List[Dict]:
        """Get evidence files by state code."""
        query = """
            SELECT * FROM evidence_files 
            WHERE evidence_transfer_state_code = %s 
            ORDER BY last_modified_utc ASC 
            LIMIT %s
        """
        return self.execute_query(query, (state_code, limit))
    
    def increment_file_retry_count(self, evidence_id: str, last_modified_process: str) -> Dict:
        """Increment file retry count."""
        query = """
            UPDATE evidence_files 
            SET retry_count = retry_count + 1, last_modified_process = %s, last_modified_utc = NOW()
            WHERE evidence_id = %s 
            RETURNING *
        """
        return self.execute_query_one(query, (last_modified_process, evidence_id))

    # Status Codes methods
    def get_status_code(self, identifier: int) -> Optional[Dict]:
        """Get status code by identifier."""
        query = "SELECT * FROM status_codes WHERE identifier = %s"
        return self.execute_query_one(query, (identifier,))
    
    def get_status_code_by_value(self, value: str) -> Optional[Dict]:
        """Get status code by value."""
        query = "SELECT * FROM status_codes WHERE value = %s"
        return self.execute_query_one(query, (value,))
    
    def get_all_status_codes(self, applies_to: str = None) -> List[Dict]:
        """Get all status codes, optionally filtered by applies_to."""
        if applies_to:
            query = """
                SELECT * FROM status_codes 
                WHERE applies_to = %s OR applies_to = 'BOTH' 
                ORDER BY ordinal
            """
            return self.execute_query(query, (applies_to,))
        else:
            query = "SELECT * FROM status_codes ORDER BY ordinal"
            return self.execute_query(query)

    # Aggregate and reporting methods
    def get_job_summary(self, job_id: str) -> Optional[Dict]:
        """Get comprehensive job summary with file counts and status info."""
        query = """
            SELECT 
                j.*,
                COUNT(ef.evidence_id) as total_files,
                COUNT(CASE WHEN ef.axon_is_downloaded = true THEN 1 END) as files_downloaded,
                COUNT(CASE WHEN ef.dems_is_transferred = true THEN 1 END) as files_transferred,
                COUNT(CASE WHEN ef.dems_is_imported = true THEN 1 END) as files_imported,
                sc.value as status_value,
                sc.description as status_description
            FROM evidence_transfer_jobs j
            LEFT JOIN evidence_files ef ON j.job_id = ef.job_id
            LEFT JOIN status_codes sc ON j.job_status_code = sc.identifier
            WHERE j.job_id = %s
            GROUP BY j.job_id, sc.value, sc.description
        """
        return self.execute_query_one(query, (job_id,))
    
    def get_files_ready_for_download(self, limit: int = 50) -> List[Dict]:
        """Get files ready for download."""
        query = """
            SELECT ef.*, j.source_system, j.source_agency 
            FROM evidence_files ef
            JOIN evidence_transfer_jobs j ON ef.job_id = j.job_id
            WHERE ef.evidence_transfer_state_code IN (30, 31) -- DOWNLOAD-READY or DOWNLOAD-READY-OVERSIZE
            AND ef.axon_is_downloaded = false
            ORDER BY ef.last_modified_utc ASC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_files_ready_for_transfer(self, limit: int = 50) -> List[Dict]:
        """Get files ready for transfer to DEMS."""
        query = """
            SELECT ef.*, j.dems_case_id 
            FROM evidence_files ef
            JOIN evidence_transfer_jobs j ON ef.job_id = j.job_id
            WHERE ef.evidence_transfer_state_code = 75 -- TRANSFER-READY
            AND ef.axon_is_downloaded = true
            AND ef.dems_is_transferred = false
            ORDER BY ef.last_modified_utc ASC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_files_ready_for_import(self, limit: int = 50) -> List[Dict]:
        """Get files ready for import into DEMS."""
        query = """
            SELECT ef.*, j.dems_case_id 
            FROM evidence_files ef
            JOIN evidence_transfer_jobs j ON ef.job_id = j.job_id
            WHERE ef.evidence_transfer_state_code = 81 -- IMPORT-REQUESTED
            AND ef.dems_is_transferred = true
            AND ef.dems_is_imported = false
            ORDER BY ef.last_modified_utc ASC
            LIMIT %s
        """
        return self.execute_query(query, (limit,))

    # Utility methods
    def health_check(self) -> Dict[str, Any]:
        """Check database health."""
        try:
            result = self.execute_query_one("SELECT NOW() as current_time")
            return {
                "healthy": True,
                "timestamp": result["current_time"].isoformat() if result else None
            }
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {"healthy": False, "error": str(e)}
    
    def close_connections(self):
        """Close all connections in the pool."""
        if self.connection_pool:
            self.connection_pool.closeall()
            self.connection_pool = None
            logger.info("Database connection pool closed")


# Singleton instance
_db_manager = None

def get_db_manager() -> DatabaseManager:
    """Get the singleton database manager instance."""
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager


# Status code constants for easy reference
class StatusCodes:
    NEW_EVIDENCE_SHARE = 10
    PENDING = 15
    VALID_CASE = 20
    INVALID_CASE = 21
    DOWNLOAD_READY = 30
    DOWNLOAD_READY_OVERSIZE = 31
    DOWNLOAD_IN_PROGRESS = 40
    DOWNLOAD_FAILED = 41
    DOWNLOAD_RETRY = 42
    DOWNLOADED = 45
    DOWNLOADED_ALL = 50
    METADATA_UPDATED = 60
    IMPORT_FILE_GENERATED = 70
    TRANSFER_READY = 75
    TRANSFERRED = 80
    IMPORT_REQUESTED = 81
    IMPORTED = 82
    IMPORT_FAILED = 83
    COMPLETED = 100
    FAILED = 110
    CANCELLED = 120
    SKIPPED = 130


# Convenience functions for direct import
def create_evidence_transfer_job(job_data: Dict[str, Any]) -> Dict:
    return get_db_manager().create_evidence_transfer_job(job_data)

def get_evidence_transfer_job(job_id: str) -> Optional[Dict]:
    return get_db_manager().get_evidence_transfer_job(job_id)

def update_job_status(job_id: str, status_code: int, job_msg: str = None, 
                     last_modified_process: str = None) -> Dict:
    return get_db_manager().update_job_status(job_id, status_code, job_msg, last_modified_process)

def update_job_counts(job_id: str, total: int = None, to_download: int = None, 
                     downloaded: int = None, last_modified_process: str = None) -> Dict:
    return get_db_manager().update_job_counts(job_id, total, to_download, downloaded, last_modified_process)

def increment_job_retry_count(job_id: str, last_modified_process: str) -> Dict:
    return get_db_manager().increment_job_retry_count(job_id, last_modified_process)

def get_jobs_by_status(status_code: int, limit: int = 100) -> List[Dict]:
    return get_db_manager().get_jobs_by_status(status_code, limit)

def set_dems_case(job_id: str, dems_case_id: str, last_modified_process: str) -> Dict:
    return get_db_manager().set_dems_case(job_id, dems_case_id, last_modified_process)

def create_evidence_file(file_data: Dict[str, Any]) -> Dict:
    return get_db_manager().create_evidence_file(file_data)

def update_evidence_file_state(evidence_id: str, state_code: int, last_modified_process: str) -> Dict:
    return get_db_manager().update_evidence_file_state(evidence_id, state_code, last_modified_process)

def mark_file_downloaded(evidence_id: str, error_msg: str = None, last_modified_process: str = None) -> Dict:
    return get_db_manager().mark_file_downloaded(evidence_id, error_msg, last_modified_process)

def mark_file_transferred(evidence_id: str, error_msg: str = None, last_modified_process: str = None) -> Dict:
    return get_db_manager().mark_file_transferred(evidence_id, error_msg, last_modified_process)

def mark_file_imported(evidence_id: str, dems_imported_id: str = None, error_msg: str = None, 
                      last_modified_process: str = None) -> Dict:
    return get_db_manager().mark_file_imported(evidence_id, dems_imported_id, error_msg, last_modified_process)

def get_evidence_file(evidence_id: str) -> Optional[Dict]:
    return get_db_manager().get_evidence_file(evidence_id)

def get_evidence_files_by_job(job_id: str) -> List[Dict]:
    return get_db_manager().get_evidence_files_by_job(job_id)

def get_evidence_files_by_state(state_code: int, limit: int = 100) -> List[Dict]:
    return get_db_manager().get_evidence_files_by_state(state_code, limit)

def increment_file_retry_count(evidence_id: str, last_modified_process: str) -> Dict:
    return get_db_manager().increment_file_retry_count(evidence_id, last_modified_process)

def get_status_code(identifier: int) -> Optional[Dict]:
    return get_db_manager().get_status_code(identifier)

def get_status_code_by_value(value: str) -> Optional[Dict]:
    return get_db_manager().get_status_code_by_value(value)

def get_all_status_codes(applies_to: str = None) -> List[Dict]:
    return get_db_manager().get_all_status_codes(applies_to)

def get_job_summary(job_id: str) -> Optional[Dict]:
    return get_db_manager().get_job_summary(job_id)

def get_files_ready_for_download(limit: int = 50) -> List[Dict]:
    return get_db_manager().get_files_ready_for_download(limit)

def get_files_ready_for_transfer(limit: int = 50) -> List[Dict]:
    return get_db_manager().get_files_ready_for_transfer(limit)

def get_files_ready_for_import(limit: int = 50) -> List[Dict]:
    return get_db_manager().get_files_ready_for_import(limit)

def health_check() -> Dict[str, Any]:
    return get_db_manager().health_check()

def execute_query(query: str, params: tuple = None) -> List[Dict]:
    return get_db_manager().execute_query(query, params)

def get_queue_urls() -> Dict[str, str]:
    return get_db_manager().get_queue_urls()