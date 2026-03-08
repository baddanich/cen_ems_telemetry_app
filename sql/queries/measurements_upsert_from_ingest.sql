INSERT INTO measurements (
    device_id,
    ts,
    metric,
    value,
    unit,
    raw_event_id,
    is_normal,
    is_reset,
    is_duplicate,
    is_late,
    is_bad,
    delta
)
VALUES (
    :device_id,
    :ts,
    :metric,
    :value,
    :unit,
    :raw_event_id,
    :is_normal,
    :is_reset,
    :is_duplicate,
    :is_late,
    :is_bad,
    :delta
)
ON CONFLICT (device_id, metric, ts) DO UPDATE SET
    value = excluded.value,
    unit = excluded.unit,
    raw_event_id = excluded.raw_event_id,
    is_duplicate = excluded.is_duplicate,
    is_late = excluded.is_late,
    is_bad = excluded.is_bad
RETURNING id, ts, value;
