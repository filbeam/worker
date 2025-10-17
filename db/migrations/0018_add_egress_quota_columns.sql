ALTER TABLE data_sets ADD COLUMN cdn_egress_quota TEXT DEFAULT '0';
ALTER TABLE data_sets ADD COLUMN cache_miss_egress_quota TEXT DEFAULT '0';
