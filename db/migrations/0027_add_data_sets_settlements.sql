CREATE TABLE data_sets_settlements (
    data_set_id TEXT PRIMARY KEY,
    usage_reported_until INTEGER DEFAULT 0,
    payments_settled_until INTEGER DEFAULT 0
);
