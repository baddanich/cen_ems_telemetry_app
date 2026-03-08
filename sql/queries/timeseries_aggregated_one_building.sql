SELECT 
    m.ts, 
    SUM(m.value) AS value, 
    SUM(COALESCE(m.delta, 0)) AS delta
FROM 
    measurements m
JOIN 
    devices d 
    ON 
        d.id = m.device_id
WHERE 
    d.building_id = :building_id {filter_clause}
GROUP BY 
    m.ts
ORDER BY 
    m.ts
