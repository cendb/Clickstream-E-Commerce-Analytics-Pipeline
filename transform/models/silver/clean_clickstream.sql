WITH raw_source AS (
    SELECT * FROM {{ ref('rp_clickstream') }}
),

cleaned as (
    SELECT
        -- Ensure unique events by hashing or selecting unique event_id
        CAST(event_id AS varchar) AS event_id,
        CAST(user_id AS varchar) AS user_id,
        CAST(product_id AS varchar) AS product_id,
        CAST(event_type AS varchar) AS event_type,
        CAST(device AS varchar) AS device,
        -- Convert string timestamp to proper DuckDB timestamp
        CAST(timestamp AS timestamp) AS event_timestamp
    FROM raw_source
    WHERE event_id IS NOT NULL
)

-- Deduplicate events just in case Kafka delivered replicas
SELECT DISTINCT * FROM cleaned
