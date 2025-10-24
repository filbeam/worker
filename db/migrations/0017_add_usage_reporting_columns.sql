ALTER TABLE data_sets ADD COLUMN usage_reported_until TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01T00:00:00.000Z' NOT NULL;
ALTER TABLE data_sets ADD COLUMN pending_usage_report_tx_hash TEXT;
