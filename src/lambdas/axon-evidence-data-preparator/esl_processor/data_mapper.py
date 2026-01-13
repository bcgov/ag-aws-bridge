import re
from datetime import datetime
from typing import Any, Dict, List, Tuple
from .models import InputCSVRow, OutputCSVRow, EvidenceCorrelation
from .exceptions import DataMappingException


class DataMapper:
    """Maps input CSV data to output CSV format"""

    def __init__(self, db_manager: Any, agency_id_code: str, logger: Any):
        self.db_manager = db_manager
        self.agency_id_code = agency_id_code
        self.logger = logger

    def map_rows(
        self,
        csv_rows: List[InputCSVRow],
        db_records: List[Dict],
        correlation_map: Dict[str, EvidenceCorrelation],
    ) -> List[OutputCSVRow]:
        """Map cleaned CSV rows with database records and correlation data to output format"""
        output_rows = []

        # Create checksum-based lookup for database records
        db_lookup = {record.get('checksum'): record for record in db_records if record.get('checksum')}

        for csv_row in csv_rows:
            db_record = db_lookup.get(csv_row.checksum)
            if not db_record:
                raise DataMappingException(
                    f"No database record found for checksum: {csv_row.checksum}"
                )

            # Get correlation data for relative_file_path
            correlation = correlation_map.get(csv_row.checksum)
            if correlation:
                csv_row.relative_file_path = correlation.filename
            else:
                self.logger.log(
                    event="correlation_missing",
                    level="WARNING",
                    message=f"No correlation data found for checksum: {csv_row.checksum}, using empty relative_file_path"
                )

            output_row = self.map_row(csv_row, db_record)
            output_rows.append(output_row)

        return output_rows

    def map_row(self, csv_row: InputCSVRow, db_record: dict) -> OutputCSVRow:
        """Map a single CSV row with database record"""
        source_case_title = db_record.get("source_case_title", "")

        # Parse AGENCY FILE NAME for eIM compliance
        is_eim_compliant, eim_parts = self._parse_agency_filename(csv_row.title)

        # Generate ID
        generated_id = self._generate_id(
            source_case_title, csv_row.title, is_eim_compliant
        )

        # Convert Shared On to DATE TO CROWN
        date_to_crown = self._convert_shared_on_to_date(csv_row.shared_on)

        # Determine RELATIVE FILE PATH from correlation data
        relative_file_path = csv_row.relative_file_path

        output_row = OutputCSVRow(
            id=generated_id,
            type="",
            description=eim_parts.get("description", ""),
            title=eim_parts.get("title", ""),
            date=eim_parts.get("date", ""),
            date_to_crown=date_to_crown,
            relative_file_path=relative_file_path,
            disclosed_status=csv_row.release_status,
            original_file_number=source_case_title,
            agency_file_name=csv_row.title,
            evidence_id=csv_row.evidence_id,
            sharing_status=csv_row.sharing_status,
            checksum=csv_row.checksum,
        )

        return output_row

    def _strip_file_extension(self, filename: str) -> str:
        """Strip file extension from filename"""
        return re.sub(r"\.[A-Za-z0-9]+$", "", filename.strip())

    def _parse_agency_filename(self, filename: str) -> Tuple[bool, Dict[str, str]]:
        """Parse agency filename using eIM compliance rules"""
        return self._check_eim_compliance(filename)

    def _check_eim_compliance(self, filename: str) -> Tuple[bool, Dict[str, str]]:
        """Check if filename is eIM compliant and extract components"""
        # Strip any file extension (e.g., .pdf) and surrounding whitespace
        filename_clean = self._strip_file_extension(filename)

        # Same logic as before - parse from right to left
        parts = filename_clean.split("_")

        if len(parts) < 3:
            return (
                False,
                {"description": filename, "title": "", "date": ""},
            )

        idx = len(parts) - 1
        potential_time = None
        potential_date = None

        # Check for TIME
        if idx >= 0 and len(parts[idx]) == 4 and parts[idx].isdigit():
            if self._is_valid_time(parts[idx]):
                potential_time = parts[idx]
                idx -= 1

        # Check for DATE
        if idx >= 0 and len(parts[idx]) in (6, 8) and parts[idx].isdigit():
            if self._is_valid_date(parts[idx]):
                potential_date = parts[idx]
                idx -= 1

        # DATE is required for compliance
        if not potential_date:
            return (
                False,
                {"description": filename, "title": "", "date": ""},
            )

        # Extract TITLE
        if idx < 0:
            return (
                False,
                {"description": filename, "title": "", "date": ""},
            )

        title = parts[idx].upper()

        # Extract DESCRIPTION
        if idx == 0:
            return (
                False,
                {"description": filename, "title": "", "date": ""},
            )

        description_parts = parts[0:idx]
        description = "_".join(description_parts).upper()

        result = {"description": description, "title": title}

        # Add date (expand 6-digit to 8-digit if needed)
        date_cleaned = potential_date.strip("()")
        if len(date_cleaned) == 6:
            # Expand 6-digit YYMMDD to 8-digit YYYYMMDD
            expanded_date = f"20{date_cleaned}"
        else:
            # Already 8-digit YYYYMMDD
            expanded_date = date_cleaned
        result["date"] = expanded_date

        if potential_time:
            result["time"] = potential_time.strip("()")

        return True, result

    def _is_valid_date(self, date_str: str) -> bool:
        """Validate date string in YYMMDD or YYYYMMDD format"""
        date_cleaned = date_str.strip("()")

        if not date_cleaned.isdigit() or len(date_cleaned) not in (6, 8):
            return False

        try:
            if len(date_cleaned) == 6:
                # YYMMDD format
                year = int(date_cleaned[0:2])
                month = int(date_cleaned[2:4])
                day = int(date_cleaned[4:6])
                # Year range: 00-99 for 6-digit
                if not (0 <= year <= 99):
                    return False
            else:
                # YYYYMMDD format
                year = int(date_cleaned[0:4])
                month = int(date_cleaned[4:6])
                day = int(date_cleaned[6:8])
                # Year range: 1900-2100 for 8-digit
                if not (1900 <= year <= 2100):
                    return False

            # Month range: 01-12
            if not (1 <= month <= 12):
                return False
            # Day range: 01-31
            if not (1 <= day <= 31):
                return False

            return True
        except (ValueError, IndexError):
            return False

    def _is_valid_time(self, time_str: str) -> bool:
        """Validate time string in HHMM format"""
        time_cleaned = time_str.strip("()")

        if not time_cleaned.isdigit() or len(time_cleaned) != 4:
            return False

        try:
            hour = int(time_cleaned[0:2])
            minute = int(time_cleaned[2:4])

            if not (0 <= hour <= 24):
                return False
            if not (0 <= minute <= 59):
                return False

            return True
        except (ValueError, IndexError):
            return False

    def _generate_id(
        self,
        source_case_title: str,
        agency_filename: str,
        is_eim_compliant: bool,
    ) -> str:
        """Generate ID: agency_id_code.YY-nnnnn.filename"""
        # Strip file extension from agency_filename
        clean_filename = self._strip_file_extension(agency_filename)
        
        # Extract YY-nnnnn from source_case_title (e.g., "PO-2025-99001" → "25-99001")
        match = re.search(r"(\d{2})-(\d+)", source_case_title)
        if match:
            yy_nnnn = f"{match.group(1)}-{match.group(2)}"
        else:
            yy_nnnn = source_case_title

        return f"{self.agency_id_code}.{yy_nnnn}.{clean_filename}"

    def _convert_shared_on_to_date(self, shared_on_str: str) -> str:
        """Convert 'Shared On' timestamp to YYYYMMDD format"""
        if not shared_on_str:
            return ""

        try:
            # Try common date formats
            formats = [
                "%b %d %Y, %H:%M",  # "Jun 10 2025, 12:33"
                "%B %d %Y, %H:%M",  # "June 10 2025, 12:33"
                "%m/%d/%Y %H:%M",   # "06/10/2025 12:33"
                "%Y-%m-%d %H:%M",   # "2025-06-10 12:33"
            ]

            for fmt in formats:
                try:
                    dt = datetime.strptime(shared_on_str.strip(), fmt)
                    return dt.strftime("%Y%m%d")
                except ValueError:
                    continue

            self.logger.log(
                event="date_conversion_failed",
                level="WARNING",
                message=f"Could not parse shared_on date: {shared_on_str}",
            )
            return ""

        except Exception as e:
            self.logger.log(
                event="date_conversion_error",
                level="ERROR",
                message=f"Error converting shared_on date: {str(e)}",
            )
            return ""
