import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .db import get_session_dep
from .models import (
    Building,
    Device,
    HealthResponse,
    IngestRequest,
    Measurement,
)
from .sql_loader import load_sql

logger = logging.getLogger(__name__)

router = APIRouter()

# Known energy units (case-insensitive)
_ENERGY_UNITS_WH = frozenset({"wh"})
_ENERGY_UNITS_KWH = frozenset({"kwh"})


def _canonical_metric_and_unit(metric: str, unit: str) -> Tuple[str, str, bool, bool]:
    """
    Returns (canonical_metric, canonical_unit, is_normal, is_bad).
    is_normal: True when we converted (e.g. Wh->kWh), False when kWh given as-is.
    is_bad: True when unit is unknown for energy (e.g. kals, not Wh/kWh).
    """
    m = metric.lower().strip()
    u = unit.lower().strip()

    if m in {"energy", "energy_total", "energy_kwh_total"}:
        if u in _ENERGY_UNITS_KWH:
            return "energy_kwh_total", "kWh", False, False  # no conversion
        if u in _ENERGY_UNITS_WH:
            return "energy_kwh_total", "kWh", True, False  # converted
        return m, unit, False, True  # unknown unit -> is_bad
    return m, unit, False, False


def _convert_value(metric: str, unit: str, value: float) -> float:
    m = metric.lower().strip()
    u = unit.lower().strip()

    if m in {"energy", "energy_total", "energy_kwh_total"}:
        if u in _ENERGY_UNITS_WH:
            return value / 1000.0
        if u in _ENERGY_UNITS_KWH:
            return value
    return value


def _compute_dedupe_key(
    device_external_id: str,
    metric: str,
    ts: datetime,
    value: float,
    explicit_key: Optional[str],
) -> str:
    if explicit_key:
        return explicit_key
    raw = f"{device_external_id}|{metric}|{ts.isoformat()}|{value}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


@router.get("/health", response_model=HealthResponse)
async def health(session: AsyncSession = Depends(get_session_dep)) -> HealthResponse:
    try:
        await session.execute(text(load_sql("health_check.sql")))
    except Exception as exc:
        logger.exception("Health check failed")
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    return HealthResponse(status="ok")


async def _get_or_create_building(session: AsyncSession, name: str) -> str:
    row = (
        await session.execute(
            text(load_sql("buildings_select_by_name.sql")),
            {"name": name},
        )
    ).mappings().first()
    if row:
        return str(row["id"])
    building_id = str(uuid.uuid4())
    await session.execute(
        text(load_sql("buildings_insert.sql")),
        {"id": building_id, "name": name},
    )
    return building_id


async def _get_or_create_device(
    session: AsyncSession,
    building_id: str,
    external_id: str,
    name: Optional[str],
) -> str:
    row = (
        await session.execute(
            text(load_sql("devices_select_by_external_id.sql")),
            {"external_id": external_id},
        )
    ).mappings().first()
    if row:
        return str(row["id"])
    device_id = str(uuid.uuid4())
    await session.execute(
        text(load_sql("devices_insert.sql")),
        {"id": device_id, "building_id": building_id, "external_id": external_id, "name": name},
    )
    return device_id


@router.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest(
    payload: IngestRequest,
    session: AsyncSession = Depends(get_session_dep),
) -> dict:
    building_id = await _get_or_create_building(session, payload.building.name)
    device_id = await _get_or_create_device(
        session,
        building_id=building_id,
        external_id=payload.device.external_id,
        name=payload.device.name,
    )

    energy_metric_used = False

    for reading in payload.readings:
        canonical_metric, canonical_unit, is_normal, is_bad = _canonical_metric_and_unit(
            reading.metric,
            reading.unit,
        )
        canonical_value = _convert_value(reading.metric, reading.unit, reading.value)

        # Raw events: store original values, dedupe on raw (no normalization)
        dedupe_key = _compute_dedupe_key(
            payload.device.external_id,
            reading.metric,
            reading.timestamp,
            reading.value,  # original value
            reading.dedupe_key,
        )
        raw_payload_str = json.dumps(reading.raw_payload) if reading.raw_payload is not None else None
        ts_str = reading.timestamp.isoformat()

        row = (
            await session.execute(
                text(load_sql("raw_events_insert_or_mark_duplicate.sql")),
                {
                    "device_id": device_id,
                    "source_ts": ts_str,
                    "metric": reading.metric,
                    "value": reading.value,
                    "unit": reading.unit,
                    "raw_payload": raw_payload_str,
                    "dedupe_key": dedupe_key,
                },
            )
        ).mappings().first()

        if row is None:
            continue

        raw_event_id = row["id"]

        latest_row = (
            await session.execute(
                text(load_sql("measurements_latest_ts_for_metric.sql")),
                {"device_id": device_id, "metric": canonical_metric},
            )
        ).mappings().first()
        max_ts = latest_row["max_ts"] if latest_row and latest_row["max_ts"] is not None else None
        max_ts_parsed = datetime.fromisoformat(max_ts.replace("Z", "+00:00")) if max_ts else None
        is_late = 1 if max_ts_parsed and reading.timestamp < max_ts_parsed else 0

        await session.execute(
            text(load_sql("measurements_upsert_from_ingest.sql")),
            {
                "device_id": device_id,
                "ts": ts_str,
                "metric": canonical_metric,
                "value": canonical_value,
                "unit": canonical_unit,
                "raw_event_id": raw_event_id,
                "is_normal": 1 if is_normal else 0,
                "is_duplicate": 0,
                "is_late": is_late,
                "is_bad": 1 if is_bad else 0,
            },
        )

        if canonical_metric == "energy_kwh_total":
            energy_metric_used = True

    if energy_metric_used:
        await session.execute(
            text(load_sql("recompute_energy_deltas.sql")),
            {"device_id": device_id, "metric": "energy_kwh_total"},
        )

    return {"status": "accepted"}


@router.get("/buildings", response_model=List[Building])
async def list_buildings(
    session: AsyncSession = Depends(get_session_dep),
) -> List[Building]:
    rows = (await session.execute(text(load_sql("buildings_list.sql")))).mappings().all()
    return [Building(id=str(r["id"]), name=r["name"]) for r in rows]


@router.get("/buildings/{building_id}/devices", response_model=List[Device])
async def list_devices(
    building_id: str,
    session: AsyncSession = Depends(get_session_dep),
) -> List[Device]:
    rows = (
        await session.execute(
            text(load_sql("devices_list_for_building.sql")),
            {"building_id": building_id},
        )
    ).mappings().all()
    return [
        Device(
            id=str(r["id"]),
            building_id=str(r["building_id"]),
            external_id=r["external_id"],
            name=r["name"],
        )
        for r in rows
    ]


def _row_to_measurement(r) -> Measurement:
    ts_val = r["ts"]
    if isinstance(ts_val, str):
        ts_val = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
    return Measurement(
        ts=ts_val,
        metric=r["metric"],
        value=float(r["value"]),
        unit=r["unit"],
        delta=float(r["delta"]) if r["delta"] is not None else None,
        is_normal=bool(r.get("is_normal", 0)),
        is_reset=bool(r.get("is_reset", 0)),
        is_duplicate=bool(r.get("is_duplicate", 0)),
        is_late=bool(r.get("is_late", 0)),
        is_bad=bool(r.get("is_bad", 0)),
    )


@router.get("/devices/{device_id}/latest", response_model=List[Measurement])
async def latest_measurements(
    device_id: str,
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    rows = (
        await session.execute(
            text(load_sql("measurements_latest_per_metric.sql")),
            {"device_id": device_id},
        )
    ).mappings().all()
    return [_row_to_measurement(r) for r in rows]


@router.get("/devices/all", response_model=List[Measurement])
async def all_devices(
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    rows = (
        await session.execute(
            text(load_sql("devices_all.sql")),
        )
    ).mappings().all()
    return [_row_to_measurement(r) for r in rows]


@router.get("/devices/{device_id}/recent", response_model=List[Measurement])
async def recent_measurements(
    device_id: str,
    metric: str = Query(...),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    rows = (
        await session.execute(
            text(load_sql("measurements_recent.sql")),
            {"device_id": device_id, "limit": limit, "offset": offset},
        )
    ).mappings().all()
    return [_row_to_measurement(r) for r in rows]


@router.get("/timeseries", response_model=List[Measurement])
async def timeseries(
    device_id: str = Query(...),
    # metric: str = Query(...),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    exclude_bad: bool = Query(True, description="Exclude is_bad records from timeseries"),
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    # conditions = ["device_id = :device_id", "metric = :metric"]
    # params: dict = {"device_id": device_id, "metric": metric}

    conditions = ["device_id = :device_id"]
    params: dict = {"device_id": device_id}

    if start is not None:
        conditions.append("ts >= :start")
        params["start"] = start.isoformat()
    if end is not None:
        conditions.append("ts <= :end")
        params["end"] = end.isoformat()
    if exclude_bad:
        conditions.append("is_bad = 0")

    where_clause = " AND ".join(conditions)
    sql = load_sql("measurements_timeseries_base.sql").format(where_clause=where_clause)
    rows = (await session.execute(text(sql), params)).mappings().all()
    return [_row_to_measurement(r) for r in rows]


@router.get("/timeseries/aggregated", response_model=List[dict])
async def timeseries_aggregated(
    building_id: str = Query(..., description="Building ID or 'all' for all buildings"),
    metric: str = Query("energy_kwh_total"),
    device_id: str = Query("all", description="Device ID or 'all' for all devices in building"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    exclude_bad: bool = Query(True),
    session: AsyncSession = Depends(get_session_dep),
) -> List[dict]:
    """
    Aggregated timeseries: sum of energy by time.
    - building_id=all, device_id=all: one series = sum across all buildings
    - building_id=X, device_id=all: one series = sum across all devices in building X
    - building_id=all: one series per building
    """
    extra = "m.is_bad = 0" if exclude_bad else ""
    params = {}
    if start:
        params["start"] = start.isoformat()
    if end:
        params["end"] = end.isoformat()

    if building_id == "all":
        sql = f"""
            SELECT m.ts, b.name AS label, SUM(m.value) AS value, SUM(COALESCE(m.delta, 0)) AS delta
            FROM measurements m
            JOIN devices d ON d.id = m.device_id
            JOIN buildings b ON b.id = d.building_id
            WHERE {extra}
        """
        if start:
            sql += " AND m.ts >= :start"
        if end:
            sql += " AND m.ts <= :end"
        sql += " GROUP BY m.ts, d.building_id, b.name ORDER BY m.ts, d.building_id"
        rows = (await session.execute(text(sql), params)).mappings().all()
        return [{"ts": r["ts"], "value": float(r["value"]), "delta": float(r["delta"] or 0), "label": r["label"]} for r in rows]

    if device_id == "all":
        sql = f"""
            SELECT m.ts, SUM(m.value) AS value, SUM(COALESCE(m.delta, 0)) AS delta
            FROM measurements m
            JOIN devices d ON d.id = m.device_id
            WHERE d.building_id = :building_id 
        """
        params["building_id"] = building_id
        if start:
            sql += " AND m.ts >= :start"
        if end:
            sql += " AND m.ts <= :end"
        sql += " GROUP BY m.ts ORDER BY m.ts"
        rows = (await session.execute(text(sql), params)).mappings().all()
        return [{"ts": r["ts"], "value": float(r["value"]), "delta": float(r["delta"] or 0), "label": "Total"} for r in rows]

    return []
