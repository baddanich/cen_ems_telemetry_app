-- One row per raw_event (original + duplicate) for Latest readings
SELECT
    m.id,
    m.ts,
    m.metric,
    m.value,
    m.unit,
    m.delta,
    m.is_normal,
    m.is_reset,
    r.is_duplicate,
    m.is_late,
    m.is_bad,
    r.id AS raw_event_id,
    m.created_at,
    m.updated_at
FROM 
    measurements m
INNER JOIN 
    raw_events r 
    ON 
        r.device_id = m.device_id 
        AND r.source_ts = m.ts
WHERE 
    m.device_id = :device_id AND {metric_condition} {bad_filter}
ORDER BY 
    m.ts DESC, r.id ASC
LIMIT 
    :limit OFFSET :offset;
