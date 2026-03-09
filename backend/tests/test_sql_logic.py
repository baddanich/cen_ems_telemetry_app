from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SQL_ROOT = Path(__file__).resolve().parents[2] / "sql" / "queries"


async def _insert_building_device(session: AsyncSession, b_id: str = "b1", d_id: str = "d1") -> None:
    """Insert a building and device for tests."""
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES (:id, :name)"),
        {"id": b_id, "name": "B1"},
    )
    await session.execute(
        text(
            "INSERT INTO devices (id, building_id, external_id, name) VALUES (:id, :bid, :ext, :name)"
        ),
        {"id": d_id, "bid": b_id, "ext": "dev-1", "name": "Device 1"},
    )
    await session.commit()


# ---------------------------------------------------------------------------
# measurements_max_ts.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_max_ts(db_pool: AsyncSession) -> None:
    """measurements_max_ts: returns max(ts) for device/metric."""
    session = db_pool
    await _insert_building_device(session)
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i in range(3):
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, 1)"
            ),
            {"did": d_id, "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(), "metric": "energy_kwh_total", "val": float(i * 10), "unit": "kWh"},
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
    await _insert_building_device(session)
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i, v in enumerate([0.0, 10.0, 25.0]):
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, 1)"
            ),
            {"did": d_id, "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(), "metric": "energy_kwh_total", "val": v, "unit": "kWh"},
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


@pytest.mark.asyncio
async def test_measurements_latest_ts_skips_bad_records(db_pool: AsyncSession) -> None:
    """measurements_latest_ts: skips is_bad=1 rows; returns previous good record for delta."""
    session = db_pool
    await _insert_building_device(session)
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    # 10:00 good 22 kWh, 10:30 bad 100 kals, 11:00 good 26 kWh
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal, is_bad) "
            "VALUES (:did, :ts, :metric, :val, :unit, 1, 0)"
        ),
        {"did": d_id, "ts": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy_kwh_total", "val": 22.0, "unit": "kWh"},
    )
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal, is_bad) "
            "VALUES (:did, :ts, :metric, :val, :unit, 0, 1)"
        ),
        {"did": d_id, "ts": (base_ts + timedelta(minutes=30)).isoformat(), "metric": "energy_kwh_total", "val": 100.0, "unit": "kals"},
    )
    await session.commit()

    sql = (_SQL_ROOT / "measurements_latest_ts.sql").read_text()
    # Query for ts just after 10:30: should return 10:00 (22), not 10:30 (100)
    query_ts = (base_ts + timedelta(minutes=31)).isoformat()
    row = (await session.execute(text(sql), {"device_id": d_id, "metric": "energy_kwh_total", "ts": query_ts})).mappings().first()
    assert row is not None
    assert float(row["value"]) == pytest.approx(22.0)


# ---------------------------------------------------------------------------
# raw_events_insert_or_mark_duplicate.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_events_insert_first_is_not_duplicate(db_pool: AsyncSession) -> None:
    """raw_events_insert: first insert with new dedupe_key returns is_duplicate=0."""
    session = db_pool
    await _insert_building_device(session)
    sql = (_SQL_ROOT / "raw_events_insert_or_mark_duplicate.sql").read_text()
    row = (
        await session.execute(
            text(sql),
            {"device_id": "d1", "source_ts": "2024-06-01T10:00:00Z", "metric": "energy", "value": 5.0, "unit": "kWh", "raw_payload": None, "dedupe_key": "key-1"},
        )
    ).mappings().first()
    await session.commit()
    assert row["id"] is not None
    assert row["is_duplicate"] == 0


@pytest.mark.asyncio
async def test_raw_events_insert_second_same_dedupe_key_is_duplicate(db_pool: AsyncSession) -> None:
    """raw_events_insert: second insert with same dedupe_key returns is_duplicate=1."""
    session = db_pool
    await _insert_building_device(session)
    sql = (_SQL_ROOT / "raw_events_insert_or_mark_duplicate.sql").read_text()
    params = {"device_id": "d1", "source_ts": "2024-06-01T10:00:00Z", "metric": "energy", "value": 5.0, "unit": "kWh", "raw_payload": None, "dedupe_key": "key-dup"}
    r1 = (await session.execute(text(sql), params)).mappings().first()
    r2 = (await session.execute(text(sql), params)).mappings().first()
    await session.commit()
    assert r1["is_duplicate"] == 0
    assert r2["is_duplicate"] == 1
    assert r1["id"] != r2["id"]


# ---------------------------------------------------------------------------
# measurement_next_by_ts.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurement_next_by_ts(db_pool: AsyncSession) -> None:
    """measurement_next_by_ts: returns closest row with ts > :ts."""
    session = db_pool
    await _insert_building_device(session)
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for i, v in enumerate([0.0, 10.0, 25.0]):
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, 1)"
            ),
            {"did": d_id, "ts": (base_ts + timedelta(minutes=i * 10)).isoformat(), "metric": "energy_kwh_total", "val": v, "unit": "kWh"},
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
    """measurements_recent_expanded: one row per raw_event (original + duplicate)."""
    session = db_pool
    await _insert_building_device(session)
    d_id = "d1"
    ts_str = "2024-06-01T10:00:00Z"
    # Insert measurement
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal, is_duplicate) "
            "VALUES (:did, :ts, :metric, :val, :unit, 1, 0)"
        ),
        {"did": d_id, "ts": ts_str, "metric": "energy_kwh_total", "val": 7.0, "unit": "kWh"},
    )
    await session.commit()
    # Two raw_events for same ts (original + duplicate)
    await session.execute(
        text(
            "INSERT INTO raw_events (device_id, source_ts, metric, value, unit, dedupe_key, is_duplicate) "
            "VALUES (:did, :ts, :m, :val, :unit, :dk, 0)"
        ),
        {"did": d_id, "ts": ts_str, "m": "energy", "val": 7.0, "unit": "kWh", "dk": "k1"},
    )
    await session.execute(
        text(
            "INSERT INTO raw_events (device_id, source_ts, metric, value, unit, dedupe_key, is_duplicate) "
            "VALUES (:did, :ts, :m, :val, :unit, :dk, 1)"
        ),
        {"did": d_id, "ts": ts_str, "m": "energy", "val": 7.0, "unit": "kWh", "dk": "k2"},
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
    assert rows[0]["is_duplicate"] == 0
    assert rows[1]["is_duplicate"] == 1
    assert rows[0]["raw_event_id"] != rows[1]["raw_event_id"]


# ---------------------------------------------------------------------------
# measurements_upsert_from_ingest.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurements_upsert_insert_then_conflict_update(db_pool: AsyncSession) -> None:
    """measurements_upsert: insert new row, then conflict updates value/is_duplicate."""
    session = db_pool
    await _insert_building_device(session)
    await session.execute(
        text(
            "INSERT INTO raw_events (device_id, source_ts, metric, value, unit, dedupe_key, is_duplicate) "
            "VALUES (:did, :ts, :m, :val, :unit, :dk, 0)"
        ),
        {"did": "d1", "ts": "2024-06-01T10:00:00Z", "m": "energy", "val": 5.0, "unit": "kWh", "dk": "r1"},
    )
    await session.commit()
    raw_id = (await session.execute(text("SELECT id FROM raw_events WHERE dedupe_key = 'r1'"))).scalar()

    sql = (_SQL_ROOT / "measurements_upsert_from_ingest.sql").read_text()
    params = {
        "device_id": "d1", "ts": "2024-06-01T10:00:00Z", "metric": "energy_kwh_total", "value": 5.0, "unit": "kWh",
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

    rows = (await session.execute(text("SELECT value, is_duplicate FROM measurements WHERE device_id = 'd1'"))).mappings().all()
    assert len(rows) == 1
    assert float(rows[0]["value"]) == pytest.approx(6.0)
    assert rows[0]["is_duplicate"] == 1


# ---------------------------------------------------------------------------
# measurement_update_delta.sql
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_measurement_update_delta(db_pool: AsyncSession) -> None:
    """measurement_update_delta: updates delta and is_reset for a row by id."""
    session = db_pool
    await _insert_building_device(session)
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_reset, is_normal) "
            "VALUES (:did, :ts, :metric, :val, :unit, NULL, 0, 1)"
        ),
        {"did": "d1", "ts": "2024-06-01T10:00:00Z", "metric": "energy_kwh_total", "val": 10.0, "unit": "kWh"},
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
    await _insert_building_device(session)
    d_id = "d1"
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    values = [0.0, 10.0, 20.0, 35.0]
    for i, v in enumerate(values):
        delta = None if i == 0 else float(v) - values[i - 1]
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {
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
    await _insert_building_device(session, "b1", "d1")
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES ('b2', 'B2')"),
    )
    await session.execute(
        text("INSERT INTO devices (id, building_id, external_id, name) VALUES ('d2', 'b2', 'dev-2', 'D2')"),
    )
    ts_str = "2024-06-01T10:00:00Z"
    for did, val, d in [("d1", 5.0, 5.0), ("d2", 3.0, 3.0)]:
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {"did": did, "ts": ts_str, "metric": "energy_kwh_total", "val": val, "unit": "kWh", "delta": d},
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
    await _insert_building_device(session)
    await session.execute(
        text("INSERT INTO devices (id, building_id, external_id, name) VALUES ('d2', 'b1', 'dev-2', 'D2')"),
    )
    ts_str = "2024-06-01T10:00:00Z"
    for did, val, d in [("d1", 5.0, 5.0), ("d2", 3.0, 3.0)]:
        await session.execute(
            text(
                "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal) "
                "VALUES (:did, :ts, :metric, :val, :unit, :delta, 1)"
            ),
            {"did": did, "ts": ts_str, "metric": "energy_kwh_total", "val": val, "unit": "kWh", "delta": d},
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
    await _insert_building_device(session)
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES ('b2', 'B2')"),
    )
    await session.execute(
        text("INSERT INTO devices (id, building_id, external_id, name) VALUES ('d2', 'b2', 'dev-2', 'D2')"),
    )
    ts_str = "2024-06-01T10:00:00Z"
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal, is_bad) "
            "VALUES ('d1', :ts, 'energy_kwh_total', 5.0, 'kWh', 5.0, 1, 0)"
        ),
        {"ts": ts_str},
    )
    await session.execute(
        text(
            "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal, is_bad) "
            "VALUES ('d2', :ts, 'energy', 999.0, 'kals', 0, 0, 1)"
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
    await _insert_building_device(session)
    await session.execute(
        text("INSERT INTO devices (id, building_id, external_id, name) VALUES ('d2', 'b1', 'dev-2', 'D2')"),
    )
    base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
    for did, vals in [("d1", [0, 10, 20]), ("d2", [0, 5, 15])]:
        for i, v in enumerate(vals):
            delta = None if i == 0 else float(v) - vals[i - 1]
            await session.execute(
                text(
                    "INSERT INTO measurements (device_id, ts, metric, value, unit, delta, is_normal) "
                    "VALUES (:did, :ts, :metric, :val, :unit, :delta, 1)"
                ),
                {"did": did, "ts": (base_ts + timedelta(minutes=i)).isoformat(), "metric": "energy_kwh_total", "val": float(v), "unit": "kWh", "delta": delta},
            )
    await session.commit()

    sql = (_SQL_ROOT / "timeseries_sum_deltas_one_building.sql").read_text()
    sql = sql.replace("{filter_clause}", "AND m.metric = :metric AND m.is_bad = 0")
    row = (await session.execute(text(sql), {"building_id": "b1", "metric": "energy_kwh_total"})).mappings().first()
    assert float(row["sum_delta"]) == pytest.approx(35.0)
