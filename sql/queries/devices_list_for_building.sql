SELECT 
    device_id AS id,
    building_id,
    device_external_id AS external_id,
    device_name AS name
FROM 
    measurements
WHERE 
    building_id = :building_id
GROUP BY
    device_id, building_id, device_external_id, device_name
ORDER BY 
    external_id;
