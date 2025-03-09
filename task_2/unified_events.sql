CREATE TABLE system_a (
    event_id BIGINT PRIMARY KEY,
    value TEXT
);

CREATE TABLE system_b (
    event_id BIGINT PRIMARY KEY,
    value TEXT,
    timestamp TIMESTAMP,
    detailed_info TEXT
);


WITH unified_events AS (
    SELECT 
        COALESCE(a.event_id, b.event_id) as event_id,
        COALESCE(b.value, a.value) as value,
        b.timestamp,
        b.detailed_info
    FULL JOIN system_b b ON a.event_id = b.event_id
)
SELECT * FROM unified_events;