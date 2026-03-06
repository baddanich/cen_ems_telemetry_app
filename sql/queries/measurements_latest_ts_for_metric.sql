SELECT max(ts) AS max_ts
FROM measurements
WHERE device_id = :device_id AND metric = :metric;
