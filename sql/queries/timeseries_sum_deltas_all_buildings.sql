SELECT 
    COALESCE(SUM(m.delta), 0) AS sum_delta
FROM 
    measurements m
WHERE 
    1=1 
    {filter_clause}
