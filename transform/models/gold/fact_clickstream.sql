SELECT
    event_id,
    user_id,
    product_id,
    event_type,
    device,
    event_timestamp
FROM {{ ref('clean_clickstream') }}