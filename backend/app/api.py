"""
CenEMS Telemetry API: ingestion, buildings/devices, timeseries, and aggregates.
"""
import json
import logging
from datetime import datetime
from typing import List, Optional

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
from .utils import (
    DbResolver,
    FilterBuilder,
    IngestUtils,
    Mappers,
    MetricNorm,
    Parsing,
)

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health", response_model=HealthResponse)
async def health(
    session: AsyncSession = Depends(get_session_dep)
) -> HealthResponse:
    """
    Health check.

    Returns 200 with `{"status": "ok"}` if the service
        and database are reachable.
    Returns 503 if the database is unavailable.
    """
    try:
        await session.execute(text(load_sql("health_check.sql")))
    except Exception as exc:
        logger.exception("Health check failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc)
        )
    return HealthResponse(status="ok")


# ---------------------------------------------------------------------------
# POST
# ---------------------------------------------------------------------------


@router.post("/ingest", status_code=status.HTTP_202_ACCEPTED)
async def ingest(
    payload: IngestRequest,
    session: AsyncSession = Depends(get_session_dep),
) -> dict:
    """
    POST: Ingest telemetry payload.

    Creates or resolves building and device by name/external_id.
    Stores raw events (with deduplication) and normalized measurements.
    Energy readings are canonicalized to `energy_kwh_total` (Wh to kWh);
    Unknown units are stored with `is_bad=1`.
    Deltas are recomputed for energy after ingest.

    Returns 202 Accepted with `{"status": "accepted"}`.
    """

    """ 0. Obtaining basic information """

    logger.info('POST: Obtaining basic information')

    building_id = await DbResolver.get_or_create_building(
        session,
        payload.building.name
    )

    device_id = await DbResolver.get_or_create_device(
        session,
        building_id=building_id,
        external_id=payload.device.external_id,
        name=payload.device.name,
    )

    for reading in payload.readings:

        """ 1. Normalization and quality flags calculation """

        logger.info('POST: Normalization and quality flags calculation')

        canonical_metric, canonical_unit, is_normal, is_bad = (
            MetricNorm.canonical_metric_and_unit(
                reading.metric,
                reading.unit,
            )
        )

        canonical_value = MetricNorm.convert_value(
            reading.metric,
            reading.unit,
            reading.value
        )

        is_late = await IngestUtils.detect_latecomer(
            session=session,
            device_id=device_id,
            canonical_metric=canonical_metric,
            raw_timestamp=reading.timestamp
        )

        delta = await IngestUtils.calculate_delta_energy(
            session=session,
            device_id=device_id,
            canonical_metric=canonical_metric,
            canonical_value=canonical_value,
            raw_timestamp=reading.timestamp
        )

        is_reset = 1 if delta == 0 else 0

        raw_timestamp_str = reading.timestamp.isoformat()

        """ 2. Deduplication """

        logger.info('POST: Deduplication')

        dedupe_key = IngestUtils.compute_dedupe_key(
            payload.device.external_id,
            reading.metric,
            reading.timestamp,
            reading.value,
            reading.dedupe_key,
        )
        """ 3. Saving raw data """

        logger.info('POST: Saving raw data')

        raw_payload_str = (
            json.dumps(reading.raw_payload) if reading.raw_payload is not None
            else None
        )

        inserted_raw = (
            await session.execute(
                text(load_sql("raw_events_insert_or_mark_duplicate.sql")),
                {
                    "device_id": device_id,
                    "source_ts": raw_timestamp_str,
                    "metric": reading.metric,
                    "value": reading.value,
                    "unit": reading.unit,
                    "raw_payload": raw_payload_str,
                    "dedupe_key": dedupe_key,
                },
            )
        ).mappings().first()

        is_duplicate = 1 if inserted_raw.get("is_duplicate") else 0

        """ 3. Saving processed data """

        logger.info('POST: Saving processed data')

        await session.execute(
            text(load_sql("measurements_upsert_from_ingest.sql")),
            {
                "device_id": device_id,
                "ts": raw_timestamp_str,
                "metric": canonical_metric,
                "value": canonical_value,
                "unit": canonical_unit,
                "raw_event_id": inserted_raw["id"],
                "is_normal": is_normal,
                "is_duplicate": is_duplicate,
                "is_late": is_late,
                "is_bad": is_bad,
                "is_reset": is_reset,
                "delta": delta
            },
        )

        """ 4. Late data processing """

        logger.info('POST: Late data processing')

        if is_late:
            await IngestUtils.process_latecomer(
                session=session,
                device_id=device_id,
                canonical_metric=canonical_metric,
                canonical_value=canonical_value,
                raw_timestamp=reading.timestamp,
            )

        logger.info('POST: Done')

    return {"status": "accepted"}


# ---------------------------------------------------------------------------
# GET
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# BUILDINGS
# ---------------------------------------------------------------------------

@router.get("/buildings", response_model=List[Building])
async def list_buildings(
    session: AsyncSession = Depends(get_session_dep),
) -> List[Building]:
    """
    List all buildings.

    Returns a list of `{id, name}`.
    Used by the UI to populate the Building filter.
    """
    rows = (await session.execute(
        text(load_sql("buildings_list.sql"))
    )).mappings().all()

    return [Building(id=str(r["id"]), name=r["name"]) for r in rows]


@router.get("/buildings/{building_id}/devices", response_model=List[Device])
async def list_devices(
    building_id: str,
    session: AsyncSession = Depends(get_session_dep),
) -> List[Device]:
    """
    List devices for a building.

    Returns a list of `{id, building_id, external_id, name}`. Used by the UI
    to populate the Device filter after selecting a building.
    """
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

# ---------------------------------------------------------------------------
# DEVICES
# ---------------------------------------------------------------------------


@router.get("/devices/all", response_model=List[Measurement])
async def all_devices(
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    """
    Latest measurements across all devices (debug/overview).

    Returns recent measurements from all devices. Useful for debugging
    or a simple overview without selecting a building/device.
    """
    rows = (
        await session.execute(
            text(load_sql("measurements_all_devices_recent.sql")),
        )
    ).mappings().all()

    return [Mappers.row_to_measurement(r) for r in rows]


@router.get("/devices/{device_id}/recent", response_model=List[Measurement])
async def recent_measurements(
    device_id: str,
    metric: str = Query(...),
    limit: int = Query(20, ge=1, le=100, description="Max number of rows"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    exclude_bad: str = Query("true"),
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    """
    Recent measurements for a device and metric (paginated).

    Returns one row per ingest event (original + duplicate),
    so the same ts can appear
    twice when a reading was ingested twice.
    Time-ordered newest first.
    When `exclude_bad=false`, includes legacy bad energy rows.
    Used by the `Latest readings` table.
    """
    exclude_bad_bool = Parsing.parse_exclude_bad(exclude_bad)

    bad_filter = " AND m.is_bad = 0" if exclude_bad_bool else ""

    metric_condition = MetricNorm.build_metric_condition(
        metric,
        include_legacy_bad=not exclude_bad_bool
    )

    metric_condition_expanded = (metric_condition.replace(
        "metric =", "m.metric =")
    ).replace("is_bad =", "m.is_bad =")

    sql = (
        load_sql("measurements_recent_expanded.sql")
        .replace("{bad_filter}", bad_filter)
        .replace("{metric_condition}", metric_condition_expanded)
    )

    rows = (
        await session.execute(
            text(sql),
            {
                "device_id": device_id,
                "metric": metric,
                "limit": limit,
                "offset": offset
            },
        )
    ).mappings().all()

    return [Mappers.row_to_measurement(r) for r in rows]


# ---------------------------------------------------------------------------
# TIMESERIES
# ---------------------------------------------------------------------------


@router.get("/timeseries", response_model=List[Measurement])
async def timeseries(
    device_id: str = Query(..., description="Device ID"),
    metric: str = Query("energy_kwh_total", description="Canonical metric"),
    start: Optional[datetime] = Query(None, description="ISO datetime"),
    end: Optional[datetime] = Query(None, description="ISO datetime"),
    exclude_bad: str = Query("true"),
    session: AsyncSession = Depends(get_session_dep),
) -> List[Measurement]:
    """
    Time-ordered measurements for a single device (and metric).

    Returns rows in ascending ts order, optionally filtered by start/end.
    When `exclude_bad=false`, includes legacy bad energy rows. Used by the
    time-series chart when a single device is selected.
    """
    exclude_bad_bool = Parsing.parse_exclude_bad(exclude_bad)

    metric_cond = MetricNorm.build_metric_condition(
        metric,
        include_legacy_bad=not exclude_bad_bool
    )

    conditions = ["device_id = :device_id", metric_cond]

    params: dict = {"device_id": device_id, "metric": metric}

    if start is not None:
        conditions.append("ts >= :start")
        params["start"] = start.isoformat()
    if end is not None:
        conditions.append("ts <= :end")
        params["end"] = end.isoformat()
    if exclude_bad_bool:
        conditions.append("is_bad = 0")

    where_clause = " AND ".join(conditions)

    sql = load_sql(
        "measurements_timeseries_base.sql"
    ).format(where_clause=where_clause)

    rows = (await session.execute(text(sql), params)).mappings().all()

    return [Mappers.row_to_measurement(r) for r in rows]


@router.get("/timeseries/aggregated", response_model=List[dict])
async def timeseries_aggregated(
    building_id: str = Query(...),
    metric: str = Query("energy_kwh_total"),
    device_id: str = Query("all"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    exclude_bad: str = Query("true"),
    frequency_minutes: int = Query(60, ge=1, le=1440,
                                   description="Time partition frequency "
                                   "in minutes (default 60)"),
    session: AsyncSession = Depends(get_session_dep),
) -> List[dict]:
    """
    Aggregated timeseries: AVG(value) per time partition (good data only).

    Partitions span min_ts to max_ts at parametrized frequency (default 30 min)
    Bad records are excluded. Use `GET /timeseries/aggregated_bad_points`
    for bad overlay.

    - `building_id=all`: one series per building (label = building name).
    - `building_id=X`, `device_id=all`: one series for that building
        (label = Total).
    """
    filter_clause, params = FilterBuilder.build_aggregated_filter(
        True, start, end, "m", metric=metric, include_legacy_bad_energy=False
    )
    params["bucket_seconds"] = frequency_minutes * 60

    if building_id == "all":
        sql = load_sql(
            "timeseries_aggregated_all_buildings.sql"
        ).format(filter_clause=filter_clause)

        rows = (await session.execute(text(sql), params)).mappings().all()

        return [
            {
                "ts": MetricNorm._ts_to_iso_utc(str(r["ts"])),
                "value": float(r["value"]),
                "delta": float(r["delta"] or 0),
                "label": r["label"]
            } for r in rows
        ]

    if device_id == "all":
        params["building_id"] = building_id

        sql = load_sql(
            "timeseries_aggregated_one_building.sql"
        ).format(filter_clause=filter_clause)

        rows = (await session.execute(text(sql), params)).mappings().all()

        return [
            {
                "ts": MetricNorm._ts_to_iso_utc(str(r["ts"])),
                "value": float(r["value"]),
                "delta": float(r["delta"] or 0),
                "label": "Total"
            } for r in rows
        ]

    return []


@router.get("/timeseries/aggregated_bad_points", response_model=List[dict])
async def timeseries_aggregated_bad_points(
    building_id: str = Query(..., description="Only 'all' is supported"),
    metric: str = Query("energy_kwh_total"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    frequency_minutes: int = Query(60, ge=1, le=1440,
                                   description="Time partition frequency"
                                   " in minutes (default 60)"),
    session: AsyncSession = Depends(get_session_dep),
) -> List[dict]:
    """
    Bad records only (is_bad=1) for overlay on the aggregated chart.

    AVG(value) per time partition per building. Used when Building=All and
    the user chooses to show bad records (gray dots).
    """
    if building_id != "all":
        return []
    time_filter, params = FilterBuilder.build_time_filter(start, end, "m")
    params["metric"] = metric
    params["bucket_seconds"] = frequency_minutes * 60
    sql = load_sql(
        "timeseries_aggregated_bad_points.sql"
    ).replace("{time_filter}", time_filter)

    rows = (await session.execute(text(sql), params)).mappings().all()

    return [
        {
            "ts": MetricNorm._ts_to_iso_utc(str(r["ts"])),
            "label": r["label"],
            "value": float(r["value"]),
            "delta": float(r["delta"] or 0)
        }
        for r in rows
    ]


@router.get("/timeseries/sum_deltas", response_model=dict)
async def timeseries_sum_deltas(
    building_id: str = Query(..., description="Building ID or 'all'"),
    device_id: str = Query("all"),
    metric: str = Query("energy_kwh_total"),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    exclude_bad: str = Query("true"),
    session: AsyncSession = Depends(get_session_dep),
) -> dict:
    """
    Sum of deltas in the given time range (good data only).

    Bad records are always excluded so the total is not affected. Returns
    `{"sum_delta": float}`. Used by the UI "Total" display.
    """
    if building_id == "all":
        filter_clause, params = FilterBuilder.build_aggregated_filter(
            exclude_bad=True,
            start=start,
            end=end,
            table_alias="m",
            metric=metric,
            include_legacy_bad_energy=False
        )

        sql = load_sql(
            "timeseries_sum_deltas_all_buildings.sql"
        ).format(filter_clause=filter_clause)

        row = (await session.execute(text(sql), params)).mappings().first()
    elif device_id == "all":
        filter_clause, params = FilterBuilder.build_aggregated_filter(
            exclude_bad=True,
            start=start,
            end=end,
            table_alias="m",
            metric=metric,
            include_legacy_bad_energy=False
        )

        params["building_id"] = building_id

        sql = load_sql(
            "timeseries_sum_deltas_one_building.sql"
        ).format(filter_clause=filter_clause)

        row = (await session.execute(text(sql), params)).mappings().first()
    else:
        filter_clause, params = FilterBuilder.build_aggregated_filter(
            exclude_bad=True,
            start=start,
            end=end,
            table_alias="",
        )

        params["device_id"] = device_id

        params["metric"] = metric

        metric_condition = "metric = :metric"

        sql = (
            load_sql("timeseries_sum_deltas_one_device.sql")
            .replace("{metric_condition}", metric_condition)
            .format(filter_clause=filter_clause)
        )

        row = (await session.execute(text(sql), params)).mappings().first()
    return {"sum_delta": float(row["sum_delta"] or 0)}
