from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SQL_ROOT = Path(__file__).resolve().parents[2] / "sql" / "queries"


def _identity_cols(
    b_id: str = "b1",
    b_name: str = "B1",
    d_id: str = "d1",
    d_ext: str = "dev-1",
    d_name: str = "Device 1",
) -> dict:
    return {
        "building_id": b_id,
        "building_name": b_name,
        "device_id": d_id,
        "device_external_id": d_ext,
        "device_name": d_name,
    }


# ---------------------------------------------------------------------------
# measurements_max_ts.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_max_ts(db_pool: AsyncSession) -> None:
    """measurements_max_ts: returns max(ts) for device/metric."""
    session = db_pool
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i in range(3):
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, 1)"
            ),
            {
                **_identity_cols(d_id=d_id),
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(),
                "metric": "energy_kwh_total",
                "val": float(i * 10),
                "unit": "kWh",
            },
        )
    await session.commit()

    sql = (_SQL_ROOT / "measurements_max_ts.sql").read_text()
    row = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total"})).mappings().first()
    assert row["max_ts"] == (base_ts + timedelta(minutes=20)).isoformat()

    row_empty = (await session.execute(text(sql), {"device_id": "d99", "metric": "energy_kwh_total"})).mappings().first()
    assert row_empty["max_ts"] is None


# ---------------------------------------------------------------------------
# measurements_latest_ts.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_latest_ts(db_pool: AsyncSession) -> None:
    """measurements_latest_ts: returns ts, value for row with max ts where ts < :ts."""
    session = db_pool
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i, v in enumerate([0.0, 10.0, 25.0]):
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, 1)"
            ),
            {
                **_identity_cols(d_id=d_id),
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
            },
        )
    await session.commit()

    sql = (_SQL_ROOT / "measurements_latest_ts.sql").read_text()
    mid = (base_ts + timedelta(minutes=15)).isoformat()
    row = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total", "ts": mid})).mappings().first()
    assert row is not None
    assert float(row["value"]) == pytest.approx(10.0)

    early = (base_ts - timedelta(minutes=1)).isoformat()
    row_none = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total", "ts": early})).mappings().first()
    assert row_none is None


# ---------------------------------------------------------------------------
# raw_events_insert_or_mark_duplicate.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_events_insert_first_is_not_duplicate(db_pool: AsyncSession) -> None:
    """raw_events_insert: inserts raw payload and returns id."""
    session = db_pool
    sql = (_SQL_ROOT / "raw_events_insert_or_mark_duplicate.sql").read_text()
    row = (
        await session.execute(
            text(sql),
            {"raw_payload": None},
        )
    ).mappings().first()
    await session.commit()
    assert row["id"] is not None


# ---------------------------------------------------------------------------
# measurement_next_by_ts.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurement_next_by_ts(db_pool: AsyncSession) -> None:
    """measurement_next_by_ts: returns closest row with ts > :ts."""
    session = db_pool
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i, v in enumerate([0.0, 10.0, 25.0]):
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, 1)"
            ),
            {
                **_identity_cols(d_id=d_id),
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
            },
        )
    await session.commit()

    sql = (_SQL_ROOT / "measurement_next_by_ts.sql").read_text()
    mid = (base_ts + timedelta(minutes=15)).isoformat()
    row = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total", "ts": mid})).mappings().first()
    assert row is not None
    assert float(row["value"]) == pytest.approx(25.0)

    late = (base_ts + timedelta(minutes=100)).isoformat()
    row_none = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total", "ts": late})).mappings().first()
    assert row_none is None


# ---------------------------------------------------------------------------
# measurements_recent_expanded.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_recent_expanded_returns_original_and_duplicate(db_pool: AsyncSession) -> None:
    """measurements_recent_expanded: returns original + duplicate measurement rows."""
    session = db_pool
    d_id = "d1"
    ts_str = "2024-06-01T10:00:00Z"
    # Insert original measurement
    await session.execute(
        text(
            "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, is_normal, is_duplicate) "
            "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, 1, 0)"
        ),
        {**_identity_cols(d_id=d_id), "did": d_id, "ts": ts_str, "metric": "energy_kwh_total", "val": 7.0, "unit": "kWh"},
    )
    # Insert duplicate measurement for same device/metric/ts
    await session.execute(
        text(
            "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, is_normal, is_duplicate) "
            "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, 1, 1)"
        ),
        {**_identity_cols(d_id=d_id), "did": d_id, "ts": ts_str, "metric": "energy_kwh_total", "val": 7.0, "unit": "kWh"},
    )
    await session.commit()

    sql = (_SQL_ROOT / "measurements_recent_expanded.sql").read_text()
    sql = sql.replace("{metric_condition}", "m.metric = :metric").replace("{bad_filter}", "")
    rows = (
        await session.execute(
            text(sql),
            {"device_id": d_id, "metric": "energy_kwh_total", "limit": 10, "offset": 0},
        )
    ).mappings().all()

    assert len(rows) == 2
    by_dup = {bool(r["is_duplicate"]): r for r in rows}
    assert by_dup[False]["value"] == pytest.approx(7.0)
    assert by_dup[True]["value"] == pytest.approx(7.0)


# ---------------------------------------------------------------------------
# measurements_upsert_from_ingest.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_upsert_insert_then_conflict_update(db_pool: AsyncSession) -> None:
    """measurements_upsert: always inserts a new row; duplicates are separate records."""
    session = db_pool
    raw_sql = (_SQL_ROOT / "raw_events_insert_or_mark_duplicate.sql").read_text()
    raw_row = (await session.execute(text(raw_sql), {"raw_payload": None})).mappings().first()
    await session.commit()
    raw_id = raw_row["id"]

    sql = (_SQL_ROOT / "measurements_upsert_from_ingest.sql").read_text()
    params = {
        **_identity_cols(b_id="b1", b_name="B1", d_id="d1", d_ext="dev-1", d_name="Device 1"),
        "device_id": "d1",
        "ts": "2024-06-01T10:00:00Z",
        "metric": "energy_kwh_total",
        "value": 5.0,
        "unit": "kWh",
        "raw_event_id": raw_id, "is_normal": 1, "is_reset": 0, "is_duplicate": 0, "is_late": 0, "is_bad": 0, "delta": None,
    }
    r1 = (await session.execute(text(sql), params)).mappings().first()
    assert r1 is not None
    assert float(r1["value"]) == pytest.approx(5.0)

    params["value"] = 6.0
    params["is_duplicate"] = 1
    params["raw_event_id"] = raw_id
    r2 = (await session.execute(text(sql), params)).mappings().first()
    assert float(r2["value"]) == pytest.approx(6.0)

    rows = (await session.execute(text("SELECT value, is_duplicate FROM measurements WHERE device_id = 'd1' ORDER BY id"))).mappings().all()
    assert len(rows) == 2
    assert float(rows[0]["value"]) == pytest.approx(5.0)
    assert rows[0]["is_duplicate"] == 0
    assert float(rows[1]["value"]) == pytest.approx(6.0)
    assert rows[1]["is_duplicate"] == 1


# ---------------------------------------------------------------------------
# measurement_update_delta.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurement_update_delta(db_pool: AsyncSession) -> None:
    """measurement_update_delta: updates delta and is_reset for a row by id."""
    session = db_pool
    await session.execute(
        text(
            "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_reset, is_normal) "
            "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, NULL, 0, 1)"
        ),
        {**_identity_cols(d_id="d1"), "did": "d1", "ts": "2024-06-01T10:00:00Z", "metric": "energy_kwh_total", "val": 10.0, "unit": "kWh"},
    )
    await session.commit()
    mid = (await session.execute(text("SELECT id FROM measurements WHERE device_id = 'd1'"))).scalar()

    sql = (_SQL_ROOT / "measurement_update_delta.sql").read_text()
    await session.execute(text(sql), {"id": mid, "delta": 10.0, "is_reset": 0})
    await session.commit()

    row = (await session.execute(text("SELECT delta, is_reset FROM measurements WHERE id = :id"), {"id": mid})).mappings().first()
    assert float(row["delta"]) == pytest.approx(10.0)
    assert row["is_reset"] == 0

    await session.execute(text(sql), {"id": mid, "delta": 0.0, "is_reset": 1})
    await session.commit()
    row = (await session.execute(text("SELECT delta, is_reset FROM measurements WHERE id = :id"), {"id": mid})).mappings().first()
    assert float(row["delta"]) == 0.0
    assert row["is_reset"] == 1


# ---------------------------------------------------------------------------
# timeseries_sum_deltas_one_device.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_sum_deltas_one_device(db_pool: AsyncSession) -> None:
    """timeseries_sum_deltas_one_device: sums deltas for device/metric."""
    session = db_pool
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    values = [0.0, 10.0, 20.0, 35.0]
    for i, v in enumerate(values):
        delta = None if i == 0 else float(v) - values[i - 1]
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {
                **_identity_cols(d_id=d_id),
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i * 15)).isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
                "delta": delta,
            },
        )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_sum_deltas_one_device.sql").read_text()
    sql = sql.replace("{metric_condition}", "metric = :metric").replace("{filter_clause}", "")
    row = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total"})).mappings().first()
    assert float(row["sum_delta"]) == pytest.approx(35.0)  # 10 + 10 + 15


# ---------------------------------------------------------------------------
# timeseries_aggregated_all_buildings.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_aggregated_all_buildings(db_pool: AsyncSession) -> None:
    """timeseries_aggregated_all_buildings: AVG(value) per time partition per building."""
    session = db_pool
    ts_str = "2024-06-01T10:00:00Z"
    for ident, did, val, d in [
        (_identity_cols(b_id="b1", b_name="B1", d_id="d1", d_ext="dev-1", d_name="D1"), "d1", 5.0, 5.0),
        (_identity_cols(b_id="b2", b_name="B2", d_id="d2", d_ext="dev-2", d_name="D2"), "d2", 3.0, 3.0),
    ]:
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {**ident, "did": did, "ts": ts_str, "metric": "energy_kwh_total", "val": val, "unit": "kWh", "delta": d},
        )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_aggregated_all_buildings.sql").read_text()
    sql = sql.replace("{filter_clause}", "AND m.metric = :metric AND m.is_bad = 0")
    rows = (await session.execute(text(sql), {"metric": "energy_kwh_total", "bucket_seconds": 1800})).mappings().all()
    assert len(rows) == 2
    by_label = {r["label"]: r for r in rows}
    assert float(by_label["B1"]["value"]) == pytest.approx(5.0)
    assert float(by_label["B1"]["delta"]) == pytest.approx(5.0)
    assert float(by_label["B2"]["value"]) == pytest.approx(3.0)
    assert float(by_label["B2"]["delta"]) == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# timeseries_aggregated_one_building.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_aggregated_one_building(db_pool: AsyncSession) -> None:
    """timeseries_aggregated_one_building: AVG(value) per time partition for one building."""
    session = db_pool
    ts_str = "2024-06-01T10:00:00Z"
    for ident, did, val, d in [
        (_identity_cols(b_id="b1", b_name="B1", d_id="d1", d_ext="dev-1", d_name="D1"), "d1", 5.0, 5.0),
        (_identity_cols(b_id="b1", b_name="B1", d_id="d2", d_ext="dev-2", d_name="D2"), "d2", 3.0, 3.0),
    ]:
        await session.execute(
            text(
                "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {**ident, "did": did, "ts": ts_str, "metric": "energy_kwh_total", "val": val, "unit": "kWh", "delta": d},
        )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_aggregated_one_building.sql").read_text()
    sql = sql.replace("{filter_clause}", "AND m.metric = :metric AND m.is_bad = 0")
    rows = (await session.execute(text(sql), {"building_id": "b1", "metric": "energy_kwh_total", "bucket_seconds": 1800})).mappings().all()
    assert len(rows) == 1
    assert float(rows[0]["value"]) == pytest.approx(4.0)  # AVG(5, 3)
    assert float(rows[0]["delta"]) == pytest.approx(8.0)  # SUM(5, 3)


# ---------------------------------------------------------------------------
# timeseries_aggregated_bad_points.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_aggregated_bad_points(db_pool: AsyncSession) -> None:
    """timeseries_aggregated_bad_points: returns is_bad=1 rows only, with label."""
    session = db_pool
    ts_str = "2024-06-01T10:00:00Z"
    await session.execute(
        text(
            "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal, is_bad) "
            "VALUES ('b1', 'B1', 'd1', 'dev-1', 'D1', :ts, 'energy_kwh_total', 5.0, 'kWh', 5.0, 1, 0)"
        ),
        {"ts": ts_str},
    )
    await session.execute(
        text(
            "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal, is_bad) "
            "VALUES ('b2', 'B2', 'd2', 'dev-2', 'D2', :ts, 'energy', 999.0, 'kals', 0, 0, 1)"
        ),
        {"ts": ts_str},
    )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_aggregated_bad_points.sql").read_text()
    sql = sql.replace("{time_filter}", "")
    rows = (await session.execute(text(sql), {"metric": "energy_kwh_total", "bucket_seconds": 1800})).mappings().all()
    assert len(rows) == 1
    assert rows[0]["label"] == "B2"
    assert float(rows[0]["value"]) == pytest.approx(999.0)


# ---------------------------------------------------------------------------
# timeseries_sum_deltas_one_building.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_timeseries_sum_deltas_one_building(db_pool: AsyncSession) -> None:
    """timeseries_sum_deltas_one_building: sums deltas for building."""
    session = db_pool
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for ident, did, vals in [
        (_identity_cols(b_id="b1", b_name="B1", d_id="d1", d_ext="dev-1", d_name="D1"), "d1", [0, 10, 20]),
        (_identity_cols(b_id="b1", b_name="B1", d_id="d2", d_ext="dev-2", d_name="D2"), "d2", [0, 5, 15]),
    ]:
        for i, v in enumerate(vals):
            delta = None if i == 0 else float(v) - vals[i - 1]
            await session.execute(
                text(
                    "INSERT INTO measurements (building_id, building_name, device_id, device_external_id, device_name, ts, metric, value, unit, delta, is_normal) "
                    "VALUES (:building_id, :building_name, :did, :device_external_id, :device_name, :ts, :metric, :val, :unit, :delta, 1)"
                ),
                {**ident, "did": did, "ts": (base_ts + timedelta(minutes=i)).isoformat(), "metric": "energy_kwh_total", "val": float(v), "unit": "kWh", "delta": delta},
            )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_sum_deltas_one_building.sql").read_text()
    sql = sql.replace("{filter_clause}", "AND m.metric = :metric AND m.is_bad = 0")
    row = (await session.execute(text(sql), {"building_id": "b1", "metric": "energy_kwh_total"})).mappings().first()
    assert float(row["sum_delta"]) == pytest.approx(35.0)
