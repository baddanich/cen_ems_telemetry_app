SELECT 
    COALESCE(SUM(m.delta), 0) AS sum_delta
FROM 
    measurements m
JOIN 
    devices d 
    ON 
        d.id = m.device_id
WHERE 
    d.building_id = :building_id {filter_clause}
