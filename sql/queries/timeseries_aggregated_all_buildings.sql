SELECT 
    m.ts, 
    b.name AS label, 
    SUM(m.value) AS value, 
    SUM(COALESCE(m.delta, 0)) AS delta
FROM 
    measurements m
JOIN 
    devices d 
    ON 
        d.id = m.device_id
JOIN 
    buildings b 
    ON 
        b.id = d.building_id
WHERE 
    1=1 {filter_clause}
GROUP BY 
    m.ts, d.building_id, b.name
ORDER BY 
    m.ts, d.building_id
