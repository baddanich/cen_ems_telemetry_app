-- Insert a new row every time; set is_duplicate=1 when this dedupe_key already exists
INSERT INTO raw_events (
    device_id,
    source_ts,
    metric,
    value,
    unit,
    raw_payload,
    dedupe_key,
    is_duplicate
)
SELECT
    :device_id,
    :source_ts,
    :metric,
    :value,
    :unit,
    :raw_payload,
    :dedupe_key,
    CASE WHEN EXISTS (SELECT 1 FROM raw_events WHERE dedupe_key = :dedupe_key) THEN 1 ELSE 0 END
RETURNING id, is_duplicate;
