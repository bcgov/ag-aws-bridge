from typing import Any, List, Dict
from lambda_structured_logger import LogLevel, LogStatus
from .models import InputCSVRow

class CSVCleaner:
    """Cleans CSV data by removing invalid/irrelevant rows"""
    
    def __init__(self, logger: Any, event: Dict[str, Any]):
        self.logger = logger
        self.event = event
    
    def clean(self, rows: List[InputCSVRow]) -> List[InputCSVRow]:
        """Execute cleaning pipeline"""
        self.logger.log(
            event=self.event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message=f"Starting CSV cleanup with {len(rows)} rows"
        )
        
        rows = self.remove_not_shared(rows)
        rows = self.remove_work_product(rows)
        
        self.logger.log(
            event=self.event,
            level=LogLevel.INFO,
            status=LogStatus.SUCCESS,
            message=f"CSV cleanup completed with {len(rows)} rows remaining"
        )
        
        return rows
    
    def remove_not_shared(self, rows: List[InputCSVRow]) -> List[InputCSVRow]:
        """Remove rows where Sharing Status = 'Not Shared'"""
        initial_count = len(rows)
        filtered_rows = [row for row in rows if row.sharing_status != "Not Shared"]
        removed_count = initial_count - len(filtered_rows)
        
        if removed_count > 0:
            self.logger.log(
                event=self.event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message=f"Removed {removed_count} 'Not Shared' rows"
            )
        
        return filtered_rows
    
    def remove_work_product(self, rows: List[InputCSVRow]) -> List[InputCSVRow]:
        """Remove rows where release_status = 'WORK PRODUCT'"""
        initial_count = len(rows)
        filtered_rows = [row for row in rows if row.release_status != "WORK PRODUCT"]
        removed_count = initial_count - len(filtered_rows)
        
        if removed_count > 0:
            self.logger.log(
                event=self.event,
                level=LogLevel.INFO,
                status=LogStatus.SUCCESS,
                message=f"Removed {removed_count} 'WORK PRODUCT' rows"
            )
        
        return filtered_rows
