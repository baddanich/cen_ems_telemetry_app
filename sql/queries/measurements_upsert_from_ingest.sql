INSERT INTO measurements (
    building_id,
    building_name,
    device_id,
    device_external_id,
    device_name,
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
    :building_id,
    :building_name,
    :device_id,
    :device_external_id,
    :device_name,
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
RETURNING id, ts, value;
