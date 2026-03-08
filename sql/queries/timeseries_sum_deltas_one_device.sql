SELECT 
    COALESCE(SUM(delta), 0) AS sum_delta
FROM 
    measurements
WHERE 
    device_id = :device_id AND {metric_condition} {filter_clause}
