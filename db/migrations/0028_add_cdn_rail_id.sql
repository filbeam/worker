-- Share the CDN bandwidth rail across a payer's data sets.
-- Bandwidth is now metered and settled per cdn_rail_id, while cache-miss stays per data set.
-- cdn_rail_id and cache_miss_rail_id come from the FWSS DataSetCreated event.
ALTER TABLE data_sets ADD COLUMN cdn_rail_id TEXT;
ALTER TABLE data_sets ADD COLUMN cache_miss_rail_id TEXT;

-- Bandwidth settlement watermark, keyed by the shared rail rather than the data set.
-- Cache-miss keeps using data_sets.cdn_payments_settled_until.
CREATE TABLE cdn_rail_settlement_state (
  cdn_rail_id TEXT PRIMARY KEY,
  cdn_payments_settled_until TIMESTAMP WITH TIME ZONE DEFAULT '1970-01-01T00:00:00.000Z' NOT NULL
);

-- Backfill the bandwidth watermark from existing per-data-set state. Ungrouped data sets
-- have a unique cdn_rail_id each, so the max collapses to the existing value per rail.
INSERT INTO cdn_rail_settlement_state (cdn_rail_id, cdn_payments_settled_until)
SELECT cdn_rail_id, MAX(cdn_payments_settled_until)
FROM data_sets
WHERE cdn_rail_id IS NOT NULL
GROUP BY cdn_rail_id;
