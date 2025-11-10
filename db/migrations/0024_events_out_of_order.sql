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
-- ALTER TABLE pieces ADD COLUMN block_number INTEGER NOT NULL;
