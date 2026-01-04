from dataclasses import dataclass
from typing import Optional


@dataclass
class InputCSVRow:
    """Data class for input CSV row from Axon API"""
    evidence_id: str
    title: str  # AGENCY FILE NAME
    shared_on: str
    sharing_status: str
    release_status: str
    checksum: str
    relative_file_path: str = ""  # Added for correlation with API filename
    evidence_group: Optional[str] = None
    owner: Optional[str] = None
    uploaded_by: Optional[str] = None
    uploaded_on: Optional[str] = None
    recorded_on: Optional[str] = None
    duration: Optional[str] = None
    category: Optional[str] = None
    status: Optional[str] = None
    parent_evidence_id: Optional[str] = None
    parent_evidence_title: Optional[str] = None
    parent_evidence_checksum: Optional[str] = None
    review_status: Optional[str] = None


@dataclass
class OutputCSVRow:
    """Data class for output CSV row (BRIDGE ESL format)"""
    id: str  # A (1)
    type: str  # B (2)
    description: str  # C (3)
    title: str  # D (4)
    date: str  # E (5)
    date_to_crown: str  # F (6)
    relative_file_path: str  # G (7)
    disclosed_status: str  # H (8)
    original_file_number: str  # I (9)
    agency_file_name: str  # J (10)
    evidence_id: str  # K (11)
    sharing_status: str  # L (12)
    checksum: str  # M (13)


@dataclass
class EvidenceCorrelation:
    """Correlation data between database records and API responses"""
    checksum: str
    filename: str
    evidence_id: str
