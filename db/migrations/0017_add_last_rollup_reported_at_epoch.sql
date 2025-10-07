-- Migration number: 0017 	 2025-10-07T00:00:00.000Z
ALTER TABLE data_sets ADD COLUMN last_rollup_reported_at_epoch INTEGER;
