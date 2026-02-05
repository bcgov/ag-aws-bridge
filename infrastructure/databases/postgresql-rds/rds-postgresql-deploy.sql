-- =====================================================
-- CREATE Tables
-- =====================================================
-- Create status code lookup table first
CREATE TABLE status_codes (
    identifier INTEGER PRIMARY KEY,
    value VARCHAR(50) NOT NULL,
    ordinal INTEGER NOT NULL,
    description TEXT,
    applies_to VARCHAR(20) NOT NULL -- 'JOB', 'FILE', or 'BOTH'
);

-- Create main tables
CREATE TABLE evidence_transfer_jobs (
    job_id VARCHAR(255) PRIMARY KEY,
    job_created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    job_status_code INTEGER REFERENCES status_codes(identifier),
    job_msg TEXT,
    source_system VARCHAR(100),
    source_agency VARCHAR(100),
    source_case_id VARCHAR(255),
    source_case_title VARCHAR(500),
    source_case_last_modified_utc TIMESTAMP,
    source_case_evidence_count_total INTEGER DEFAULT 0,
    source_case_evidence_count_to_download INTEGER DEFAULT 0,
    source_case_evidence_count_downloaded INTEGER DEFAULT 0,
    dems_case_id VARCHAR(255),
    last_modified_process VARCHAR(100),
    last_modified_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
);

CREATE TABLE evidence_files (
    evidence_id VARCHAR(255) PRIMARY KEY,
    job_id VARCHAR(255) REFERENCES evidence_transfer_jobs(job_id),
    evidence_transfer_state_code INTEGER REFERENCES status_codes(identifier),
    evidence_file_id VARCHAR(255),
    evidence_file_type VARCHAR(100),
    source_case_id VARCHAR(255),
    file_size_bytes BIGINT,
    checksum VARCHAR(255),
    axon_is_downloaded BOOLEAN DEFAULT FALSE,
    axon_download_error_msg TEXT,
    axon_download_utc TIMESTAMP,
    dems_is_transferred BOOLEAN DEFAULT FALSE,
    dems_transfer_error_msg TEXT,
    dems_transferred_utc TIMESTAMP,
    dems_is_imported BOOLEAN DEFAULT FALSE,
    dems_imported_error_msg TEXT,
    dems_imported_utc TIMESTAMP,
    dems_imported_id VARCHAR(255),
    last_modified_process VARCHAR(100),
    last_modified_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retry_count INTEGER DEFAULT 0,
	bridge_s3_cleanup_scheduled_date TIMESTAMP,
	bridge_s3_cleanup_scheduled BOOLEAN,
	bridge_s3_cleanup_completed BOOLEAN,
	bridge_s3_cleanup_completed_date TIMESTAMP,
	axon_evidence_category_id INTEGER,
	axon_evidence_category_change_date TIMESTAMP
);

-- =====================================================
-- CREATE INDEXES
-- =====================================================
-- Indexes for status_codes table
CREATE INDEX idx_status_codes_ordinal ON status_codes(ordinal);
CREATE INDEX idx_status_codes_applies_to ON status_codes(applies_to);
CREATE INDEX idx_status_codes_value ON status_codes(value);

-- Indexes for evidence_transfer_jobs table
CREATE INDEX idx_jobs_status ON evidence_transfer_jobs(job_status_code);
CREATE INDEX idx_jobs_created ON evidence_transfer_jobs(job_created_utc);
CREATE INDEX idx_jobs_modified ON evidence_transfer_jobs(last_modified_utc);
CREATE INDEX idx_jobs_source_case ON evidence_transfer_jobs(source_case_id);
CREATE INDEX idx_jobs_source_system ON evidence_transfer_jobs(source_system);
CREATE INDEX idx_jobs_retry_count ON evidence_transfer_jobs(retry_count);

-- Indexes for evidence_files table
CREATE INDEX idx_files_job_id ON evidence_files(job_id);
CREATE INDEX idx_files_state ON evidence_files(evidence_transfer_state_code);
CREATE INDEX idx_files_evidence_id ON evidence_files(evidence_file_id);
CREATE INDEX idx_files_source_case ON evidence_files(source_case_id);
CREATE INDEX idx_files_download_status ON evidence_files(axon_is_downloaded);
CREATE INDEX idx_files_transfer_status ON evidence_files(dems_is_transferred);
CREATE INDEX idx_files_import_status ON evidence_files(dems_is_imported);
CREATE INDEX idx_files_modified ON evidence_files(last_modified_utc);
CREATE INDEX idx_files_download_method ON evidence_files(download_method);
CREATE INDEX idx_files_file_type ON evidence_files(evidence_file_type);
CREATE INDEX idx_files_retry_count ON evidence_files(retry_count);

-- Composite indexes for common queries
CREATE INDEX idx_jobs_status_created ON evidence_transfer_jobs(job_status_code, job_created_utc);
CREATE INDEX idx_files_job_state ON evidence_files(job_id, evidence_transfer_state_code);
CREATE INDEX idx_files_state_modified ON evidence_files(evidence_transfer_state_code, last_modified_utc);

-- =====================================================
-- INSERT STATUS CODES
-- =====================================================
-- Insert unified status codes
INSERT INTO status_codes VALUES
-- Early stages (both job and file level)
(10, 'NEW-EVIDENCE-SHARE', 10, 'New or updated case shared', 'JOB'),
(15, 'PENDING', 15, 'Pending downloading and processing', 'FILE'),
(20, 'VALID-CASE', 20, 'Case information validated and matched', 'JOB'),
(21, 'INVALID-CASE', 21, 'Case information not valid or matched', 'JOB'),

-- Download preparation
(30, 'DOWNLOAD-READY', 30, 'Ready for download', 'BOTH'),
(31, 'DOWNLOAD-READY-OVERSIZE', 31, 'Ready for download - oversize', 'BOTH'),

-- Download process
(40, 'DOWNLOAD-IN-PROGRESS', 40, 'Download in progress', 'BOTH'),
(41, 'DOWNLOAD-FAILED', 41, 'Download failed', 'BOTH'),
(42, 'DOWNLOAD-RETRY', 42, 'Download retry in progress', 'BOTH'),
(45, 'DOWNLOADED', 45, 'File downloaded successfully', 'FILE'),
(50, 'DOWNLOADED-ALL', 50, 'All evidence downloaded (job only)', 'JOB'),

-- Metadata updates
(60, 'METADATA-UPDATED', 60, 'Metadata updated', 'BOTH'),

-- File generation and transfer preparation
(70, 'IMPORT-FILE-GENERATED', 70, 'DEMS import file generated', 'JOB'),
(75, 'TRANSFER-READY', 75, 'Ready for transfer', 'FILE'),

-- Transfer process
(80, 'TRANSFERRED', 80, 'Transferred to DEMS', 'BOTH'),
(81, 'IMPORT-REQUESTED', 81, 'Import requested', 'BOTH'),
(82, 'IMPORTED', 82, 'Successfully imported', 'BOTH'),
(83, 'IMPORT-FAILED', 83, 'Import failed', 'BOTH'),
(84, 'IMPORTED-WITH-ERRORS', 140,'Import completed with errors or warnings','BOTH'),
(22, 'INVALID-AGENCY-IDENTIFIER', '22', 'Axon case title does not match BCPS agency listing', 'JOB');

-- Final states
(100, 'COMPLETED', 100, 'Processing completed successfully', 'BOTH'),
(110, 'FAILED', 110, 'Processing failed', 'BOTH'),
(120, 'CANCELLED', 120, 'Cancelled by user/system', 'BOTH'),
(130, 'SKIPPED', 130, 'Skipped (file only)', 'FILE');