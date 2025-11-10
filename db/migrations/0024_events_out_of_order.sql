CREATE TABLE pieces_tmp (
  id TEXT NOT NULL,
  data_set_id TEXT NOT NULL,
  cid TEXT, -- Remove NOT NULL constraint
  ipfs_root_cid STRING,
  PRIMARY KEY (id, data_set_id)
);
INSERT INTO pieces_tmp SELECT * FROM pieces;
DROP INDEX pieces_cid;
DROP TABLE pieces;
ALTER TABLE pieces_tmp RENAME TO pieces;
CREATE INDEX pieces_cid ON pieces(cid);

ALTER TABLE pieces ADD COLUMN is_deleted BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE data_sets_tmp (
  id TEXT NOT NULL,
  service_provider_id TEXT, -- Remove NOT NULL constraint
  payer_address TEXT, -- Remove NOT NULL constraint
  with_cdn BOOLEAN NOT NULL,
  total_egress_bytes_used INTEGER NOT NULL DEFAULT 0,
  terminate_service_tx_hash TEXT,
  with_ipfs_indexing BOOLEAN NOT NULL DEFAULT FALSE,
  usage_reported_until TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01T00:00:00.000Z' NOT NULL,
  pending_usage_report_tx_hash TEXT,
  lockup_unlocks_at TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (id)
);
INSERT INTO data_sets_tmp SELECT * FROM data_sets;
DROP TABLE data_sets;
ALTER TABLE data_sets_tmp RENAME TO data_sets;
