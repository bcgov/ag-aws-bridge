ALTER TABLE evidence_files
	ADD COLUMN evidence_file_name VARCHAR(255) ;
COMMENT ON COLUMN evidence_files.evidence_file_name IS 'Storage for the evidence file name returned from ';

