SELECT 
    id, 
    value
FROM 
    measurements
WHERE 
    device_id = :device_id AND metric = :metric AND ts > :ts AND is_duplicate IS FALSE and is_bad IS FALSE
ORDER BY 
    ts ASC, id ASC
LIMIT 1;
