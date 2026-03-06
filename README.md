## CenEMS Telemetry Service

This project implements a small telemetry ingestion and normalization service for an energy management platform, plus a lightweight React UI.

### Architecture

- **Backend**: FastAPI (Python) with SQLAlchemy async, backed by SQLite.
- **Database**:
  - `raw_events`: as-ingested telemetry (no normalization), deterministic deduplication via `dedupe_key`.
  - `measurements`: normalized canonical measurements with derived `delta` and quality flags (`is_normal`, `is_reset`, `is_duplicate`, `is_late`, `is_bad`).
  - SQL schema and logic live in `sql/schema.sql`, `sql/functions.sql`, and `sql/queries/*.sql`.
- **Derived metrics**:
  - `recompute_energy_deltas.sql` recomputes deltas in timestamp order. Negative deltas are recorded as 0 with `is_reset=1`.
  - Unit conversion is case-insensitive (Wh/kWh). Unknown units set `is_bad=1`.
- **Frontend**: Vite + React UI with buildings/devices, latest readings (Quality Flags), time-series chart, and aggregated views (All buildings / All devices).

### Running with Docker (recommended)

```bash
docker-compose up --build
```

Backend runs on `http://localhost:8000` with SQLite in a Docker volume.

### Running backend locally (Pipenv)

```bash
pipenv install
pipenv run dev
```

### Running tests

```bash
pipenv run pytest backend/tests -vv
```

### Running the frontend

```bash
cd frontend && npm install && npm run dev
```

UI proxies API calls to `http://localhost:8000`.

### API overview

- `POST /ingest` – ingest building, device, and readings.
- `GET /buildings` – list buildings.
- `GET /buildings/{id}/devices` – list devices.
- `GET /devices/{id}/latest` – latest readings with quality flags.
- `GET /timeseries?device_id=...&metric=...` – time-series (excludes `is_bad` by default).
- `GET /timeseries/aggregated?building_id=...&device_id=...&metric=...` – aggregated energy by building or device.
- `GET /health` – health check.

---

### Sample data: 3 buildings, 2+ sensors each, 10+ records

Use these curl commands to populate sample data including late, non-normalized, reset, and bad records.

**Building 1 – North Campus (sensors: meter-n1, meter-n2)**

```bash
# Sensor meter-n1: 12 records, includes Wh (normalized), late, reset
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "North Campus"},
  "device": {"external_id": "meter-n1", "name": "North Main"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:15:00Z", "metric": "energy", "value": 5.2, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 10.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:45:00Z", "metric": "energy", "value": 15.1, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 20.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:15:00Z", "metric": "energy", "value": 25.3, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 30.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:20:00Z", "metric": "energy", "value": 27.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:45:00Z", "metric": "energy", "value": 5.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 10.0, "unit": "Wh"},
    {"timestamp": "2024-03-01T10:15:00Z", "metric": "energy", "value": 500000, "unit": "Wh"},
    {"timestamp": "2024-03-01T10:15:00Z", "metric": "energy", "value": 500000, "unit": "Wh"}
  ]
}'

# Sensor meter-n2: 10 records, includes bad unit (kals)
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "North Campus"},
  "device": {"external_id": "meter-n2", "name": "North Sub"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 3.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 6.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 10.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 14.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:30:00Z", "metric": "energy", "value": 18.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T11:00:00Z", "metric": "energy", "value": 22.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T11:30:00Z", "metric": "energy", "value": 100, "unit": "kals"},
    {"timestamp": "2024-03-01T12:00:00Z", "metric": "energy", "value": 26.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T12:30:00Z", "metric": "energy", "value": 30.0, "unit": "kWh"}
  ]
}'
```

**Building 2 – South Campus (sensors: meter-s1, meter-s2)**

```bash
# Sensor meter-s1: 11 records, includes Wh, Wh (normalized)
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "South Campus"},
  "device": {"external_id": "meter-s1", "name": "South Main"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "Wh"},
    {"timestamp": "2024-03-01T08:15:00Z", "metric": "energy", "value": 2000, "unit": "Wh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 4000, "unit": "Wh"},
    {"timestamp": "2024-03-01T08:45:00Z", "metric": "energy", "value": 6000, "unit": "Wh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 8000, "unit": "Wh"},
    {"timestamp": "2024-03-01T09:15:00Z", "metric": "energy", "value": 10000, "unit": "Wh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 12000, "unit": "Wh"},
    {"timestamp": "2024-03-01T09:45:00Z", "metric": "energy", "value": 14000, "unit": "Wh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 16000, "unit": "Wh"},
    {"timestamp": "2024-03-01T10:15:00Z", "metric": "energy", "value": 18000, "unit": "Wh"},
    {"timestamp": "2024-03-01T10:30:00Z", "metric": "energy", "value": 20000, "unit": "Wh"}
  ]
}'

# Sensor meter-s2: 10 records
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "South Campus"},
  "device": {"external_id": "meter-s2", "name": "South Sub"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 1.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 3.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 4.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 6.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:30:00Z", "metric": "energy", "value": 7.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T11:00:00Z", "metric": "energy", "value": 9.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T11:30:00Z", "metric": "energy", "value": 10.5, "unit": "kWh"},
    {"timestamp": "2024-03-01T12:00:00Z", "metric": "energy", "value": 12.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T12:30:00Z", "metric": "energy", "value": 13.5, "unit": "kWh"}
  ]
}'
```

**Building 3 – East Campus (sensors: meter-e1, meter-e2)**

```bash
# Sensor meter-e1: 12 records, includes reset, late
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "East Campus"},
  "device": {"external_id": "meter-e1", "name": "East Main"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:15:00Z", "metric": "energy", "value": 5.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 10.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:45:00Z", "metric": "energy", "value": 15.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 20.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:15:00Z", "metric": "energy", "value": 25.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 30.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:45:00Z", "metric": "energy", "value": 2.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 7.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:20:00Z", "metric": "energy", "value": 22.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:15:00Z", "metric": "energy", "value": 12.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:30:00Z", "metric": "energy", "value": 17.0, "unit": "kWh"}
  ]
}'

# Sensor meter-e2: 10 records, includes bad unit
curl -X POST http://localhost:8000/ingest -H "Content-Type: application/json" -d '{
  "building": {"name": "East Campus"},
  "device": {"external_id": "meter-e2", "name": "East Sub"},
  "readings": [
    {"timestamp": "2024-03-01T08:00:00Z", "metric": "energy", "value": 0, "unit": "kWh"},
    {"timestamp": "2024-03-01T08:30:00Z", "metric": "energy", "value": 2.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:00:00Z", "metric": "energy", "value": 4.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T09:30:00Z", "metric": "energy", "value": 6.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:00:00Z", "metric": "energy", "value": 8.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T10:30:00Z", "metric": "energy", "value": 999, "unit": "kals"},
    {"timestamp": "2024-03-01T11:00:00Z", "metric": "energy", "value": 12.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T11:30:00Z", "metric": "energy", "value": 14.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T12:00:00Z", "metric": "energy", "value": 16.0, "unit": "kWh"},
    {"timestamp": "2024-03-01T12:30:00Z", "metric": "energy", "value": 18.0, "unit": "kWh"}
  ]
}'
```

**Summary:** 3 buildings, 6 sensors, 65+ records with late (09:20 after 09:30), reset (30→2), normalized (Wh→kWh), duplicate (same ts/value), and bad records (kals).
