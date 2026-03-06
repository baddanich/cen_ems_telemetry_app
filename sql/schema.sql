-- Database schema for CenEMS telemetry service (SQLite)

-- Buildings represent physical sites
CREATE TABLE IF NOT EXISTS buildings (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Devices belong to buildings and emit telemetry
CREATE TABLE IF NOT EXISTS devices (
    id           TEXT PRIMARY KEY,
    building_id  TEXT NOT NULL REFERENCES buildings(id) ON DELETE CASCADE,
    external_id  TEXT NOT NULL UNIQUE,
    name         TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Raw events are stored as-ingested for traceability (no normalization)
CREATE TABLE IF NOT EXISTS raw_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    source_ts    TEXT NOT NULL,
    ingested_at  TEXT NOT NULL DEFAULT (datetime('now')),
    metric       TEXT NOT NULL,
    value        REAL NOT NULL,
    unit         TEXT NOT NULL,
    raw_payload  TEXT,
    dedupe_key   TEXT NOT NULL UNIQUE
);

-- Normalized measurements in canonical units and timestamps
CREATE TABLE IF NOT EXISTS measurements (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id     TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
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
    UNIQUE (device_id, metric, ts)
);

CREATE INDEX IF NOT EXISTS measurements_device_metric_time_idx
    ON measurements(device_id, metric, ts);
