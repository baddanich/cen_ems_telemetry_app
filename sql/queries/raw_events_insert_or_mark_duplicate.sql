INSERT INTO raw_events (
    device_id,
    source_ts,
    metric,
    value,
    unit,
    raw_payload,
    dedupe_key
)
VALUES (:device_id, :source_ts, :metric, :value, :unit, :raw_payload, :dedupe_key)
ON CONFLICT (dedupe_key) DO NOTHING
RETURNING id;
