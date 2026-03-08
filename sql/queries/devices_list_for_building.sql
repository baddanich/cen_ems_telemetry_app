SELECT 
    id, 
    building_id, 
    external_id, 
    name
FROM 
    devices
WHERE 
    building_id = :building_id
ORDER BY 
    external_id;
