## CenEMS Telemetry Service

This project implements a small telemetry ingestion and normalization service for an energy management platform, plus a lightweight React UI.

### Status
The base requirements were delivered, with handling of bad records, late events, deduplication, and aggregated views. The main focus is quality and simplicity.

**Limitations and areas for improvement:**
- ~~Rolling window parameters are not configurable~~
- ~~Missing support for different aggregation types (sum/avg)~~
- Static graph with no interactivity
- Insufficient test coverage
- UI endpoints need better organization
- Out-of-index logic is naive, though it covers the specific use case


### Architecture

- **Backend**: FastAPI (Python) with SQLAlchemy async, backed by SQLite.
- **Frontend**: Vite + React UI with Building/Device filters, **Energy** metric only, time range, time-series chart (with zoom), Mode (raw/delta), Scale (kWh/MWh/GWh), bad records toggle, latest readings table, and aggregated views (Building=All or Device=All).

### Requirements and justifications

Each requirement is listed with a short justification for why it exists.

### Core

| # | Requirement | Justification |
|---|-------------|---------------|
| R1 | **Ingestion & raw events** — `POST /ingest` stores every reading in `raw_events` and writes normalized rows to `measurements`. | Preserves audit trail; supports replay and debugging without losing original payloads. |
| R2 | **Deduplication** — Deterministic `dedupe_key`; on conflict we still return the row and set `is_duplicate=1` (raw_events, measurement keeps flags). | Duplicate ingest is recorded with `is_duplicate=1` instead of dropped; idempotent and avoids double-counting in deltas/totals. |
| R3 | **Canonical metric & unit** — Energy is normalized to `energy_kwh_total`; Wh is converted to kWh; unknown units (e.g. kals) set `is_bad=1` but are still stored. | One consistent series for queries and UI; bad data is visible or hideable instead of dropped. |
| R4 | **Quality flags** — Each measurement has `is_normal`, `is_reset`, `is_duplicate`, `is_late`, `is_bad`. | Enables filtering and highlighting of suspect data; totals and charts can exclude bad data by default. |
| R5 | **Out of order handling** — late events handling | The delta is computed using the simple relationship `delta = value[i] - value[i-1]`. The chosen strategy is to recalculate only `value[i+1]` when late updates occur, as this is the sole value affected by the delta dependency. |

### UI 

| # | Requirement | Justification |
|---|-------------|---------------|
| R1 | **Buildings & devices** — Buildings and devices are created on first use (by name / external_id). List endpoints: `GET /buildings`, `GET /buildings/{id}/devices`. | Simple hierarchy for filtering in the UI; no separate provisioning step. |
| R2 | **Time range** — Optional start/end for timeseries, aggregated, and sum_deltas. | Lets users focus on a window; totals and charts stay consistent with the chosen range. |
| R3 | **Recent & timeseries** — `GET /devices/{id}/recent` (paginated, newest first) and `GET /timeseries` (ascending, optional start/end). Query `exclude_bad` to include or hide bad records. | Supports "latest readings" table and time-series chart; same API serves both good-only and "show bad" views. |
| R4 | **Aggregated views** — Building=All: one series per building; Building=X: one "Total" series. AVG(value) per time partition (parametrized `frequency_minutes`, default 60). Good data only; bad points fetched separately for overlay. Timestamps returned as ISO UTC (Z suffix) for consistent display with raw data. | Multi-building comparison and single-building total; time-aligned partitions avoid timezone drift; UTC timestamps ensure frontend parses correctly across timezones. |
| R5 | **Total** — `GET /timeseries/sum_deltas` returns total consumption in the range (good data only). | Single "Total" figure for the selected period and scope. |
| R6 | **Health check** — `GET /health` returns 200 when DB is reachable, 503 otherwise. | Enables load balancers and orchestration to probe readiness. |
| R7 | **UI: Building/Device filters, Energy metric, time range** — Frontend allows selecting building (or All), device (pr All), and optional start/end. | Matches backend capabilities and keeps the UI aligned with the data model. |
| R8 | **UI: Time-series** — Chart shows raw values, deltas, rolling avg and sum (Mode selector); zoom and Reset apply to the visible point range only. | Large datasets remain navigable; Mode lets users switch between cumulative and incremental view. |
| R9 | **UI: Bad records toggle** — User can show or hide bad points; Building=All uses aggregated bad points overlay (no "Bad: building" in legend). | Visibility of bad data when needed without cluttering the legend or affecting totals. |


### Running with Docker (recommended)

```bash
docker-compose up --build
```

Backend runs on `http://localhost:8000` with SQLite in a Docker volume.


### Running the frontend

```bash
cd frontend && npm install && npm run dev
```

UI proxies API calls to `http://localhost:5173/`.

### Running tests

Backend tests use pytest and the shared SQL schema; DB is in-memory SQLite. Tests import `create_app` from `backend.app.main` (the app factory stays in the app package so production and tests share one definition).

```bash
pipenv run pytest backend/tests -vv
```

- `test_api_integration.py`: health, ingest, buildings/devices, latest, timeseries, recent with `exclude_bad`, time range filter, sum_deltas excludes bad, duplicate dedupe_key → `is_duplicate=1`, late out-of-order and incremental delta, aggregated timeseries and aggregated_bad_points.
- `test_sql_logic.py`: delta recomputation, reset flag, duplicate handling, aggregated timeseries (time partitions, AVG per bucket).
- `test_utils.py`: unit tests for `Parsing`, `MetricNorm`, `IngestUtils`, `FilterBuilder`, `Mappers`, and `DbResolver` (get_or_create building/device).

### Running backend locally (Pipenv)

```bash
pipenv install
pipenv run dev
```

### API reference

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check; returns 200 `{"status":"ok"}` or 503 if DB unavailable. |
| POST | `/ingest` | Ingest building, device, and readings; 202 Accepted. Normalizes energy to `energy_kwh_total`, stores raw events with dedupe. |
| GET | `/buildings` | List all buildings `[{id, name}]`. |
| GET | `/buildings/{building_id}/devices` | List devices for a building `[{id, building_id, external_id, name}]`. |
| GET | `/devices/all` | Latest measurements across all devices (debug). |
| GET | `/devices/{device_id}/recent` | Recent measurements for device and metric; query: `metric`, `limit`, `offset`, `exclude_bad` ('true'/\'false'). Paginated, newest first. |
| GET | `/timeseries` | Time-ordered measurements for one device; query: `device_id`, `metric`, `start`, `end`, `exclude_bad`. |
| GET | `/timeseries/aggregated` | AVG(value) per time partition (good data only); query: `building_id`, `device_id`, `metric`, `start`, `end`, `exclude_bad`, `frequency_minutes` (1–1440, default 60). Timestamps in ISO UTC. |
| GET | `/timeseries/aggregated_bad_points` | Bad records only for overlay; query: `building_id=all`, `metric`, `start`, `end`, `frequency_minutes` (1–1440, default 60). Timestamps in ISO UTC. |
| GET | `/timeseries/sum_deltas` | Sum of deltas in range (good data only); query: `building_id`, `device_id`, `metric`, `start`, `end`. |

Backend logic lives in `backend/app/api.py` (routes and ingest flow) and `backend/app/utils.py` (parsing, normalization, filter-building). Each endpoint has a docstring in `api.py`.

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
