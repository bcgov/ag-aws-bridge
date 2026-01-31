from typing import Any, Dict, Tuple, List

from lambda_structured_logger.enums import LogLevel, LogStatus
from .exceptions import CSVValidationException
from .csv_reader import InputCSVReader
from .csv_cleaner import CSVCleaner
from .data_mapper import DataMapper
from .csv_writer import OutputCSVWriter
from .evidence_correlator import EvidenceCorrelator

class ESLProcessor:
    """Main orchestrator for ESL CSV processing pipeline"""
    
    def __init__(self, db_manager: Any, agency_id_code: str, logger: Any, event: Dict[str, Any], base_url: str = None, bearer_token: str = None, agency_id: str = None):
        self.db_manager = db_manager
        self.agency_id_code = agency_id_code
        self.logger = logger
        self.event = event
        self.base_url = base_url
        self.bearer_token = bearer_token
        self.agency_id = agency_id
        
        self.reader = InputCSVReader(logger, event)
        self.cleaner = CSVCleaner(logger, event)
        self.mapper = DataMapper(db_manager, agency_id_code, logger, event)
        self.writer = OutputCSVWriter(logger, event)
        self.correlator = EvidenceCorrelator(base_url, bearer_token, agency_id, logger, event) if all([base_url, bearer_token, agency_id]) else None
    
    def process(
        self, 
        input_csv_path: str, 
        output_csv_path: str,
        job_id: str
    ) -> Tuple[bool, str]:
        """
        Process ESL CSV: read → clean → correlate → validate → map → write
        
        Returns:
            Tuple of (success: bool, message: str)
        """
        try:
            self.logger.log(
                event=self.event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message=f"Starting ESL processing for job_id: {job_id}"
            )
            
            # Step 1: Read input CSV
            csv_rows = self.reader.read(input_csv_path)
            initial_row_count = len(csv_rows)
            
            # Step 2: Clean CSV
            cleaned_rows = self.cleaner.clean(csv_rows)
            cleaned_row_count = len(cleaned_rows)
            
            # Step 3: Fetch database records and build correlation
            db_records = self._fetch_db_records(job_id)
            correlation_map = self._build_evidence_correlation(db_records)
            
            # Step 4: Validate - cleaned rows match database records via checksum
            self._validate_row_counts(cleaned_row_count, len(db_records))
            if self.correlator:
                self._validate_correlation(cleaned_rows, correlation_map)
            else:
                self.logger.log(
                    event=self.event,
                    level=LogLevel.WARNING,
                    status=LogStatus.WARNING,
                    message="Correlation validation skipped - no API credentials provided"
                )
            
            # Step 5: Map data with correlation
            output_rows = self.mapper.map_rows(cleaned_rows, db_records, correlation_map)
            
            # Step 6: Write output CSV
            self.writer.write(output_csv_path, output_rows)
            
            message = (
                f"ESL processing completed successfully. "
                f"Input: {initial_row_count} rows, "
                f"Cleaned: {cleaned_row_count} rows, "
                f"Correlated: {len(correlation_map)} records, "
                f"Output: {len(output_rows)} rows"
            )
            
            self.logger.log(
                event=self.event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message=message
            )
            
            return True, message
            
        except Exception as e:
            error_msg = f"ESL processing failed: {str(e)}"
            self.logger.log(
                event=self.event,
                level=LogLevel.ERROR,
                status=LogStatus.FAILURE,
                message=error_msg,
                context_data={"error_type": type(e).__name__}
            )
            raise
    
    def _fetch_db_records(self, job_id: str) -> List[Dict]:
        """Fetch database records for job"""
        evidence_files = self.db_manager.get_evidence_files_by_job(job_id)
        return evidence_files
    
    def _build_evidence_correlation(self, db_records: List[Dict]) -> Dict[str, Any]:
        """Build correlation map between database records and API data"""
        if not self.correlator:
            self.logger.log(
                event=self.event,
                level=LogLevel.WARNING,
                status=LogStatus.WARNING,
                message="EvidenceCorrelator not initialized - missing API credentials"
            )
            return {}
        
        return self.correlator.build_correlation_map(db_records)
    
    def _validate_correlation(self, csv_rows: List[Any], correlation_map: Dict[str, Any]) -> None:
        """Validate that CSV rows can be correlated with database records via checksum"""
        missing_correlations = []
        
        # Create a case-insensitive lookup map
        correlation_map_lower = {k.lower(): v for k, v in correlation_map.items()}
        
        for csv_row in csv_rows:
            if csv_row.checksum.lower() not in correlation_map_lower:
                missing_correlations.append(csv_row.checksum)
        
        if missing_correlations:
            raise CSVValidationException(
                f"Correlation validation failed: {len(missing_correlations)} CSV rows could not be correlated with database records. "
                f"Missing checksums: {missing_correlations[:5]}{'...' if len(missing_correlations) > 5 else ''}"
            )
    
    def _validate_row_counts(self, cleaned_count: int, db_count: int) -> None:
        """Validate that cleaned CSV rows match database records"""
        if cleaned_count != db_count:
            raise CSVValidationException(
                f"Row count mismatch after cleanup: "
                f"CSV has {cleaned_count} rows, "
                f"Database has {db_count} records"
            )