UPDATE 
    measurements
SET 
    delta = :delta, is_reset = :is_reset
WHERE 
    id = :id;
