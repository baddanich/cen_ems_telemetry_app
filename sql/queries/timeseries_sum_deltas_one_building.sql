SELECT 
    COALESCE(SUM(m.delta), 0) AS sum_delta
FROM 
    measurements m
WHERE 
    m.building_id = :building_id {filter_clause}
