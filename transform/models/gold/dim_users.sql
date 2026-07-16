SELECT
    user_id,
    count(event_id) as total_interactions,
    count(case when event_type = 'purchase' then 1 end) as total_purchases,
    max(event_timestamp) as last_active_at,
    -- Determine their most frequently used device
    mode(device) as preferred_device
FROM {{ ref('clean_clickstream') }}
GROUP BY 1