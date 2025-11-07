CREATE TABLE data_set_egress_quotas (
    data_set_id TEXT PRIMARY KEY,
    cdn_egress_quota INTEGER DEFAULT 0,
    cache_miss_egress_quota INTEGER DEFAULT 0
);

INSERT INTO data_set_egress_quotas (data_set_id, cdn_egress_quota, cache_miss_egress_quota)
SELECT id, cdn_egress_quota, cache_miss_egress_quota
FROM data_sets
WHERE with_cdn = 1;

ALTER TABLE data_sets DROP COLUMN cdn_egress_quota;
ALTER TABLE data_sets DROP COLUMN cache_miss_egress_quota;
