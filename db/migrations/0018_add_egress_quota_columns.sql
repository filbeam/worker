-- Migration number: 0018 	 2025-10-06T00:00:00.000Z
ALTER TABLE data_sets ADD COLUMN cdn_egress_quota INTEGER;
ALTER TABLE data_sets ADD COLUMN cache_miss_egress_quota INTEGER;
