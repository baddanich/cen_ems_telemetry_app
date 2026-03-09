-- Time partition: bucket ts by frequency (default 30 min). AVG(value) per partition per building.
WITH bucketed AS (
    SELECT
        m.value,
        m.delta,
        d.building_id,
        b.name AS label,
        datetime((strftime('%s', m.ts) / :bucket_seconds) * :bucket_seconds, 'unixepoch') AS partition_ts
    FROM measurements m
    JOIN devices d ON d.id = m.device_id
    JOIN buildings b ON b.id = d.building_id
    WHERE 1=1 {filter_clause}
)
SELECT
    partition_ts AS ts,
    label,
    AVG(value) AS value,
    SUM(COALESCE(delta, 0)) AS delta
FROM bucketed
GROUP BY partition_ts, building_id, label
ORDER BY partition_ts, building_id;
