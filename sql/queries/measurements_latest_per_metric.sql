SELECT id, ts, metric, value, unit, delta, is_normal, is_reset, is_duplicate, is_late, is_bad
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY metric ORDER BY ts DESC) AS rn
    FROM measurements
    WHERE device_id = :device_id
) sub
WHERE rn = 1
ORDER BY metric;
