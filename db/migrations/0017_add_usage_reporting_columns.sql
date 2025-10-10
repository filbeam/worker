-- Migration number: 0017 	 2025-10-07T00:00:00.000Z
ALTER TABLE data_sets ADD COLUMN usage_reported_until  TIMESTAMP WITH TIME ZONE;
ALTER TABLE data_sets ADD COLUMN pending_rollup_tx_hash TEXT;
