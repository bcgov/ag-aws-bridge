-- Add new column 
ALTER TABLE evidence_files
	ADD COLUMN evidence_id_source varchar(255) DEFAULT '' NOT NULL;
	

	