-- Negative deltas recorded as 0, is_reset set to 1
-- is_normal is set at ingestion (unit conversion), not overwritten here
WITH ordered AS (
    SELECT
        id,
        value,
        lag(value) OVER (ORDER BY ts, id) AS prev_value
    FROM measurements
    WHERE device_id = :device_id AND metric = :metric
),
computed AS (
    SELECT
        id,
        CASE
            WHEN prev_value IS NULL THEN NULL
            WHEN value >= prev_value THEN value - prev_value
            ELSE 0
        END AS new_delta,
        CASE
            WHEN prev_value IS NULL THEN 0
            WHEN value < prev_value THEN 1
            ELSE 0
        END AS new_reset
    FROM ordered
)
UPDATE measurements
SET
    delta = computed.new_delta,
    is_reset = computed.new_reset
FROM computed
WHERE measurements.id = computed.id;
