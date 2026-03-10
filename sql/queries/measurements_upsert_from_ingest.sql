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
    delta,
    updated_at
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
    :delta,
    CASE WHEN :is_late = 1 OR :is_duplicate = 1 THEN datetime('now') ELSE NULL END
)
ON CONFLICT (device_id, metric, ts) DO UPDATE SET
    value = excluded.value,
    unit = excluded.unit,
    raw_event_id = excluded.raw_event_id,
    is_duplicate = excluded.is_duplicate,
    is_late = excluded.is_late,
    is_bad = excluded.is_bad,
    updated_at = CASE WHEN excluded.is_late = 1 OR excluded.is_duplicate = 1 THEN datetime('now') ELSE measurements.updated_at END
RETURNING id, ts, value;
