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
    d.id AS device_id,
    COALESCE(d.external_id, d.name, d.id) AS label
FROM
    measurements m
JOIN
    devices d ON d.id = m.device_id
WHERE
    {where_clause}
ORDER BY
    d.id ASC,
    m.ts ASC,
    m.id ASC;

