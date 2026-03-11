SELECT 
    building_id AS id,
    building_name AS name
FROM 
    measurements
GROUP BY
    building_id, building_name
ORDER BY 
    name ASC;
