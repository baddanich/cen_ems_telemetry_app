-- Time partition: bucket ts by frequency (default 30 min). AVG(value) per partition per building for bad points only.
WITH bucketed AS (
    SELECT
        m.value,
        m.delta,
        m.building_id,
        m.building_name AS label,
        datetime((strftime('%s', m.ts) / :bucket_seconds) * :bucket_seconds, 'unixepoch') AS partition_ts
    FROM measurements m
    WHERE (m.metric = :metric OR (m.metric = 'energy' AND m.is_bad = 1))
      AND m.is_bad = 1
      AND m.is_duplicate = 0
      {time_filter}
)
SELECT
    partition_ts AS ts,
    label,
    AVG(value) AS value,
    SUM(COALESCE(delta, 0)) AS delta
FROM bucketed
GROUP BY partition_ts, building_id, label
ORDER BY partition_ts, building_id, label;
