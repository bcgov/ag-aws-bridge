-- Add new columns to evidence_transfer_jobs

ALTER TABLE evidence_transfer_jobs 
	ADD COLUMN agency_id_code varchar(20) DEFAULT '' NOT NULL ,
	ADD COLUMN agency_file_number varchar(255) DEFAULT '' NOT NULL;
	
