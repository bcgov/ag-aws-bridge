ALTER TABLE evidence_files
	ADD COLUMN bridge_s3_cleanup_scheduled_date TIMESTAMP ;
COMMENT ON COLUMN evidence_files.bridge_s3_cleanup_scheduled_date IS 'When the EventBridge S3-cleanup event is scheduled for';

ALTER TABLE evidence_files
	ADD COLUMN bridge_s3_cleanup_scheduled BOOLEAN ;
COMMENT ON COLUMN evidence_files.bridge_s3_cleanup_scheduled IS 'Whether the EventBridge S3-cleanup event is scheduled';

ALTER TABLE evidence_files
	ADD COLUMN bridge_s3_cleanup_completed BOOLEAN ;
COMMENT ON COLUMN evidence_files.bridge_s3_cleanup_completed IS 'Whether the EventBridge S3-cleanup event is completed';

ALTER TABLE evidence_files
	ADD COLUMN bridge_s3_cleanup_completed_date TIMESTAMP ;
COMMENT ON COLUMN evidence_files.bridge_s3_cleanup_completed_date IS 'When the EventBridge S3-cleanup event was completed';

ALTER TABLE evidence_files
	ADD COLUMN axon_evidence_category_id INTEGER ;
COMMENT ON COLUMN evidence_files.axon_evidence_category_id IS 'Axon evidence category ID (for retention and deletion internal in Axon)';

ALTER TABLE evidence_files
	ADD COLUMN axon_evidence_category_change_date TIMESTAMP ;
COMMENT ON COLUMN evidence_files.axon_evidence_category_change_date IS 'When the evidence files were changed to a specific category type (via Category ID) for deletion thereafter (internal in Axon)';

