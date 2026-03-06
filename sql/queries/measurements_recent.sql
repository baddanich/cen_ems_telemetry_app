SELECT ts, metric, value, unit, delta, is_normal, is_reset, is_duplicate, is_late, is_bad
FROM measurements
WHERE device_id = :device_id AND is_bad = 0
ORDER BY ts DESC
LIMIT :limit OFFSET :offset;
