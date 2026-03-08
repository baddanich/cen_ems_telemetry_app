SELECT COALESCE(SUM(m.delta), 0) AS sum_delta
FROM measurements m
JOIN devices d ON d.id = m.device_id
WHERE 1=1 {filter_clause}
