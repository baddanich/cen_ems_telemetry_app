-- Database schema for CenEMS telemetry service (SQLite)

-- Raw events are stored as-ingested for traceability (no normalization)
CREATE TABLE IF NOT EXISTS raw_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_payload  TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Denormalized measurements in canonical units and timestamps
-- (building/device identity is stored on every measurement row)
CREATE TABLE IF NOT EXISTS measurements (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    building_id   TEXT NOT NULL,
    building_name TEXT NOT NULL,
    device_id     TEXT NOT NULL,
    device_external_id TEXT NOT NULL,
    device_name   TEXT,
    ts            TEXT NOT NULL,
    metric        TEXT NOT NULL,
    value         REAL NOT NULL,
    unit          TEXT NOT NULL,
    is_normal     INTEGER NOT NULL DEFAULT 0,
    is_reset      INTEGER NOT NULL DEFAULT 0,
    is_duplicate  INTEGER NOT NULL DEFAULT 0,
    is_late       INTEGER NOT NULL DEFAULT 0,
    is_bad        INTEGER NOT NULL DEFAULT 0,
    raw_event_id  INTEGER REFERENCES raw_events(id) ON DELETE SET NULL,
    delta         REAL,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT
);

CREATE INDEX IF NOT EXISTS measurements_device_metric_time_idx
    ON measurements(device_id, metric, ts);

CREATE INDEX IF NOT EXISTS measurements_building_device_idx
    ON measurements(building_id, device_id);
