-- Creates a view that scans all Parquet files dynamically
SELECT
    event_id,
    user_id,
    product_id,
    event_type,
    device,
    timestamp
FROM read_parquet('../data/raw/*.parquet')