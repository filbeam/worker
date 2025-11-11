DROP TABLE pieces;
CREATE TABLE pieces (
  id TEXT NOT NULL,
  data_set_id TEXT NOT NULL,
  cid TEXT, -- Remove NOT NULL constraint
  ipfs_root_cid STRING,
  is_deleted BOOLEAN NOT NULL DEFAULT false, -- Add column
  PRIMARY KEY (id, data_set_id)
);
CREATE INDEX pieces_cid ON pieces(cid);

DROP TABLE data_sets;
CREATE TABLE data_sets (
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

DELETE FROM service_providers;
ALTER TABLE service_providers ADD COLUMN block_number INTEGER;
