SELECT 
    id, 
    value
FROM 
    measurements
WHERE 
    device_id = :device_id AND metric = :metric AND ts > :ts
ORDER BY 
    ts ASC, id ASC
LIMIT 1;
