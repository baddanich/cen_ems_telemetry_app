SELECT 
    ts,
    value
FROM 
    measurements
WHERE 
    device_id = :device_id AND metric = :metric and ts < :ts
ORDER BY
    ts DESC
LIMIT 1;