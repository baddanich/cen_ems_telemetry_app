SELECT 
    max(ts) as max_ts
FROM 
    measurements
WHERE 
    device_id = :device_id AND metric = :metric AND is_duplicate IS FALSE and is_bad IS FALSE;