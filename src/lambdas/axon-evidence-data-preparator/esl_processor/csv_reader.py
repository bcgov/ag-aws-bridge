import csv
from typing import List, Any
from .models import InputCSVRow
from .exceptions import CSVReadException

class InputCSVReader:
    """Reads and parses input CSV from Axon API"""
    
    REQUIRED_HEADERS = [
        'Evidence ID', 'Title', 'Shared On', 'Sharing Status', 
        'Evidence Checksum', 'release_status'
    ]
    
    def __init__(self, logger: Any):
        self.logger = logger
    
    def read(self, filepath: str) -> List[InputCSVRow]:
        """Read input CSV file and parse into InputCSVRow objects"""
        try:
            rows = []
            
            with open(filepath, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate headers
                if reader.fieldnames is None:
                    raise CSVReadException("CSV file is empty or has no headers")
                
                self._validate_headers(reader.fieldnames)
                
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is 1)
                    try:
                        csv_row = self._parse_row(row, row_num)
                        rows.append(csv_row)
                    except Exception as e:
                        raise CSVReadException(f"Error parsing row {row_num}: {str(e)}")
            
            self.logger.log(
                event="input_csv_read_success",
                level="INFO",
                message=f"Successfully read {len(rows)} rows from input CSV",
                context_data={"rows_count": len(rows)}
            )
            
            return rows
            
        except CSVReadException:
            raise
        except Exception as e:
            raise CSVReadException(f"Failed to read CSV file {filepath}: {str(e)}")
    
    def _validate_headers(self, headers: List[str]) -> None:
        """Validate that CSV has all required headers"""
        missing_headers = set(self.REQUIRED_HEADERS) - set(headers)
        if missing_headers:
            raise CSVReadException(f"Missing required headers: {missing_headers}")
    
    def _parse_row(self, row: dict, row_num: int) -> InputCSVRow:
        """Parse a CSV row dictionary into InputCSVRow object"""
        evidence_id = row.get('Evidence ID', '').strip()
        if not evidence_id:
            raise CSVReadException(f"Row {row_num}: Evidence ID is required and cannot be empty")
        
        return InputCSVRow(
            evidence_id=evidence_id,
            title=row.get('Title', '').strip(),
            shared_on=row.get('Shared On', '').strip(),
            sharing_status=row.get('Sharing Status', '').strip(),
            release_status=row.get('release_status', '').strip(),
            checksum=row.get('Evidence Checksum', '').strip(),
            evidence_group=row.get('Evidence Group', '').strip() or None,
            owner=row.get('Owner', '').strip() or None,
            uploaded_by=row.get('Uploaded By', '').strip() or None,
            uploaded_on=row.get('Uploaded On', '').strip() or None,
            recorded_on=row.get('Recorded On', '').strip() or None,
            duration=row.get('Duration', '').strip() or None,
            category=row.get('Category', '').strip() or None,
            status=row.get('Status', '').strip() or None,
            parent_evidence_id=row.get('Parent Evidence ID', '').strip() or None,
            parent_evidence_title=row.get('Parent Evidence Title', '').strip() or None,
            parent_evidence_checksum=row.get('Parent Evidence Checksum', '').strip() or None,
            review_status=row.get('review_status', '').strip() or None,
        )
