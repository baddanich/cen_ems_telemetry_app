SELECT 
    id,
    building_id,
    building_name,
    device_id,
    device_external_id,
    device_name,
    ts,
    metric,
    value,
    unit,
    delta,
    is_normal,
    is_reset,
    is_duplicate,
    is_late,
    is_bad
FROM
    measurements
ORDER BY 
    ts DESC
