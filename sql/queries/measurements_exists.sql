SELECT 1 AS exists_flag
FROM measurements
WHERE device_id = :device_id
  AND metric = :metric
  AND ts = :ts
LIMIT 1;

