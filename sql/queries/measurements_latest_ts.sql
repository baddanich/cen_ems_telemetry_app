-- Previous good (is_bad=0) record for delta calculation.
-- Bad records are skipped so delta = value[i] - value[i-1] uses the last valid reading.
SELECT 
    ts,
    value
FROM 
    measurements
WHERE 
    device_id = :device_id AND metric = :metric AND ts < :ts AND is_bad = 0
ORDER BY
    ts DESC
LIMIT 1;