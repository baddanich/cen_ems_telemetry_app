SELECT m.ts, b.name AS label, m.value AS value, COALESCE(m.delta, 0) AS delta
FROM measurements m
JOIN devices d ON d.id = m.device_id
JOIN buildings b ON b.id = d.building_id
WHERE (m.metric = :metric OR (m.metric = 'energy' AND m.is_bad = 1))
  AND m.is_bad = 1
  {time_filter}
ORDER BY m.ts, d.building_id, b.name
