-- Time partition: bucket ts by frequency (default 30 min). AVG(value) per partition for one building.
WITH bucketed AS (
    SELECT
        m.value,
        m.delta,
        datetime((strftime('%s', m.ts) / :bucket_seconds) * :bucket_seconds, 'unixepoch') AS partition_ts
    FROM measurements m
    WHERE m.building_id = :building_id {filter_clause}
)
SELECT
    partition_ts AS ts,
    AVG(value) AS value,
    SUM(COALESCE(delta, 0)) AS delta
FROM bucketed
GROUP BY partition_ts
ORDER BY partition_ts;
