import csv
from typing import Any, List, Dict
from lambda_structured_logger import LogLevel, LogStatus
from .models import OutputCSVRow
from .exceptions import CSVWriteException

class OutputCSVWriter:
    """Writes output CSV in BRIDGE ESL format"""
    
    COLUMN_HEADERS = [
        'ID', 'TYPE', 'DESCRIPTION', 'TITLE', 'DATE', 'DATE TO CROWN',
        'RELATIVE FILE PATH', 'DISCLOSED STATUS', 'ORIGINAL FILE NUMBER',
        'AGENCY FILE NAME', 'EVIDENCE ID', 'SHARING STATUS', 'CHECKSUM',
        'EVIDENCE ID STAGING', 'EVIDENCE FILE ID STAGING'
    ]
    
    def __init__(self, logger: Any, event: Dict[str, Any]):
        self.logger = logger
        self.event = event
    
    def write(self, filepath: str, rows: List[OutputCSVRow]) -> bool:
        """Write output CSV file"""
        try:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.COLUMN_HEADERS)
                writer.writeheader()
                
                for row in rows:
                    writer.writerow({
                        'ID': row.id,
                        'TYPE': row.type,
                        'DESCRIPTION': row.description,
                        'TITLE': row.title,
                        'DATE': row.date,
                        'DATE TO CROWN': row.date_to_crown,
                        'RELATIVE FILE PATH': row.relative_file_path,
                        'DISCLOSED STATUS': row.disclosed_status,
                        'ORIGINAL FILE NUMBER': row.original_file_number,
                        'AGENCY FILE NAME': row.agency_file_name,
                        'EVIDENCE ID': row.evidence_id,
                        'SHARING STATUS': row.sharing_status,
                        'CHECKSUM': row.checksum,
                        'EVIDENCE ID STAGING': row.evidence_id_staging,
                        'EVIDENCE FILE ID STAGING': row.evidence_file_id_staging,
                    })
            
            self.logger.log(
                event=self.event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message=f"Successfully wrote {len(rows)} rows to output CSV",
                context_data={"filepath": filepath, "rows_count": len(rows)}
            )
            
            return True
            
        except Exception as e:
            raise CSVWriteException(f"Failed to write CSV file {filepath}: {str(e)}")
