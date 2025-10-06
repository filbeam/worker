-- Migration number: 0018 	 2025-01-06T00:00:00.000Z
-- Add egress quota columns to data_sets table
-- Using TEXT to store large numbers since uint256 can exceed SQLite INTEGER limits
ALTER TABLE data_sets ADD COLUMN cdn_egress_quota TEXT;
ALTER TABLE data_sets ADD COLUMN cache_miss_egress_quota TEXT;