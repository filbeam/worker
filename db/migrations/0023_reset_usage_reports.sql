-- Reset existing usage reports due to migration from FilBeamOperator v1.0.1 to v1.0.2 
-- We are not able to settle usage reported to the FilBeamOperator v1.0.1 so we should re-report full usage
UPDATE 
    data_sets
SET 
    usage_reported_until = '1970-01-01T00:00:00.000Z',
    pending_usage_report_tx_hash = NULL;
