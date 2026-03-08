SELECT id, ts, metric, value, unit, delta, is_normal, is_reset, is_duplicate, is_late, is_bad
FROM measurements
WHERE device_id = :device_id AND {metric_condition} {bad_filter}
ORDER BY ts DESC
LIMIT :limit OFFSET :offset
