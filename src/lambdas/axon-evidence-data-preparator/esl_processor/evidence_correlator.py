import json
import urllib3
from typing import Dict, List, Optional, Any
from .models import EvidenceCorrelation
from .exceptions import DataMappingException


class EvidenceCorrelator:
    """Handles correlation between database evidence files and API responses"""

    def __init__(self, base_url: str, bearer_token: str, agency_id: str, logger: Any):
        self.base_url = base_url
        self.agency_id = agency_id
        self.logger = logger
        self.http = urllib3.PoolManager()
        self.headers = {
            'Authorization': f'Bearer {bearer_token}',
            'Content-Type': 'application/json'
        }

    def build_correlation_map(self, db_records: List[Dict]) -> Dict[str, EvidenceCorrelation]:
        """
        Build correlation map from database records to API data.
        Uses checksum as the correlation key since evidence_id may not match.
        """
        correlation_map = {}

        for db_record in db_records:
            evidence_id = db_record.get('evidence_id')
            checksum = db_record.get('checksum')
            evidence_file_id = db_record.get('evidence_file_id')

            if not evidence_id or not checksum:
                self.logger.log(
                    event="correlation_missing_data",
                    level="WARNING",
                    message=f"Missing evidence_id or checksum in database record: {db_record}",
                )
                continue

            # Make API call to get evidence details
            api_data = self._fetch_evidence_details(evidence_id)
            if not api_data:
                continue

            # Extract filename from API response
            filename = self._extract_filename_from_api(api_data, evidence_file_id)
            if not filename:
                self.logger.log(
                    event="correlation_filename_not_found",
                    level="WARNING",
                    message=f"No filename found for evidence_id: {evidence_id}, file_id: {evidence_file_id}",
                )
                continue

            # Create correlation entry
            correlation = EvidenceCorrelation(
                checksum=checksum,
                filename=filename,
                evidence_id=evidence_id
            )

            correlation_map[checksum] = correlation

            self.logger.log(
                event="correlation_built",
                level="INFO",
                message=f"Built correlation for checksum: {checksum[:16]}..., filename: {filename}",
            )

        return correlation_map

    def _fetch_evidence_details(self, evidence_id: str) -> Optional[Dict]:
        """Fetch evidence details from Axon API"""
        try:
            url = f"{self.base_url}/api/v2/agencies/{self.agency_id}/evidence/{evidence_id}"

            self.logger.log(
                event="api_call_start",
                level="INFO",
                message=f"Making API call for evidence_id: {evidence_id}",
            )

            response = self.http.request('GET', url, headers=self.headers, timeout=30.0)

            if response.status != 200:
                self.logger.log(
                    event="api_call_error",
                    level="ERROR",
                    message=f"API call failed for evidence_id: {evidence_id}, status: {response.status}",
                )
                return None

            return json.loads(response.data.decode('utf-8'))

        except Exception as e:
            self.logger.log(
                event="api_call_exception",
                level="ERROR",
                message=f"Exception during API call for evidence_id: {evidence_id}: {str(e)}",
            )
            return None

    def _extract_filename_from_api(self, api_data: Dict, evidence_file_id: str) -> Optional[str]:
        """
        Extract filename from API response.
        Only matches by evidence_file_id.
        """
        try:
            evidence_data = api_data.get('data', {})
            relationships = evidence_data.get('relationships', {})
            files_data = relationships.get('files', {}).get('data', [])

            # Only try to match by evidence_file_id
            for file_data in files_data:
                if file_data.get('id') == evidence_file_id:
                    attributes = file_data.get('attributes', {})
                    return attributes.get('fileName')

            # No match found
            return None

        except Exception as e:
            self.logger.log(
                event="filename_extraction_error",
                level="ERROR",
                message=f"Error extracting filename from API data: {str(e)}",
            )

        return None