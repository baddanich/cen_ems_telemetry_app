SELECT
    m.ts,
    m.metric,
    m.value,
    m.unit,
    m.delta,
    m.is_normal,
    m.is_reset,
    m.is_duplicate,
    m.is_late,
    m.is_bad,
    m.device_id AS device_id,
    COALESCE(m.device_external_id, m.device_name, m.device_id) AS label
FROM
    measurements m
WHERE
    {where_clause}
ORDER BY
    m.device_id ASC,
    m.ts ASC,
    m.id ASC;

