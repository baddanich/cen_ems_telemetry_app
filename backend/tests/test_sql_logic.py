from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SQL_ROOT = Path(__file__).resolve().parents[2] / "sql" / "queries"


@pytest.mark.asyncio
async def test_energy_deltas_monotonic(db_pool: AsyncSession) -> None:
    session = db_pool
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES (:id, :name)"),
        {"id": "b1", "name": "B1"},
    )
    b_id = "b1"
    await session.execute(
        text(
            "INSERT INTO devices (id, building_id, external_id, name) VALUES (:id, :bid, :ext, :name)"
        ),
        {"id": "d1", "bid": b_id, "ext": "dev-1", "name": "Device 1"},
    )
    d_id = "d1"

    base_ts = datetime.now(timezone.utc)
    values = [0.0, 10.0, 25.0]
    for i, v in enumerate(values):
        await session.execute(
            text(
                """
                INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
                VALUES (:did, :ts, :metric, :val, :unit, 1)
                """
            ),
            {
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i)).isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
            },
        )
    await session.commit()

    await session.execute(
        text((_SQL_ROOT / "recompute_energy_deltas.sql").read_text()),
        {"device_id": d_id, "metric": "energy_kwh_total"},
    )
    await session.commit()

    rows = (
        await session.execute(
            text(
                """
                SELECT value, delta, is_reset, is_normal
                FROM measurements
                WHERE device_id = :did AND metric = 'energy_kwh_total'
                ORDER BY ts ASC
                """
            ),
            {"did": d_id},
        )
    ).mappings().all()

    assert [float(r["value"]) for r in rows] == values
    deltas = [r["delta"] for r in rows]
    assert deltas[0] is None
    assert float(deltas[1]) == pytest.approx(10.0)
    assert float(deltas[2]) == pytest.approx(15.0)
    assert [bool(r["is_reset"]) for r in rows] == [False, False, False]
    assert [bool(r["is_normal"]) for r in rows] == [True, True, True]


@pytest.mark.asyncio
async def test_energy_counter_reset_sets_flag_and_zero_delta(db_pool: AsyncSession) -> None:
    session = db_pool
    b_id = "b2"
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES (:id, :name)"),
        {"id": b_id, "name": "B2"},
    )
    d_id = "d2"
    await session.execute(
        text(
            "INSERT INTO devices (id, building_id, external_id, name) VALUES (:id, :bid, :ext, :name)"
        ),
        {"id": d_id, "bid": b_id, "ext": "dev-2", "name": "Device 2"},
    )

    base_ts = datetime.now(timezone.utc)
    series = [10.0, 20.0, 5.0]
    for i, v in enumerate(series):
        await session.execute(
            text(
                """
                INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
                VALUES (:did, :ts, :metric, :val, :unit, 1)
                """
            ),
            {
                "did": d_id,
                "ts": (base_ts + timedelta(minutes=i)).isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
            },
        )
    await session.commit()

    await session.execute(
        text((_SQL_ROOT / "recompute_energy_deltas.sql").read_text()),
        {"device_id": d_id, "metric": "energy_kwh_total"},
    )
    await session.commit()

    rows = (
        await session.execute(
            text(
                """
                SELECT value, delta, is_reset, is_normal
                FROM measurements
                WHERE device_id = :did AND metric = 'energy_kwh_total'
                ORDER BY ts ASC
                """
            ),
            {"did": d_id},
        )
    ).mappings().all()

    deltas = [r["delta"] for r in rows]
    resets = [bool(r["is_reset"]) for r in rows]
    normals = [bool(r["is_normal"]) for r in rows]

    assert deltas[0] is None
    assert float(deltas[1]) == pytest.approx(10.0)
    assert float(deltas[2]) == 0.0  # negative delta recorded as 0
    assert resets == [False, False, True]
    # is_normal is set at ingestion (unit conversion), recompute does not change it
    assert normals == [True, True, True]


@pytest.mark.asyncio
async def test_out_of_order_inserts_recomputed_correctly(db_pool: AsyncSession) -> None:
    session = db_pool
    b_id = "b3"
    await session.execute(
        text("INSERT INTO buildings (id, name) VALUES (:id, :name)"),
        {"id": b_id, "name": "B3"},
    )
    d_id = "d3"
    await session.execute(
        text(
            "INSERT INTO devices (id, building_id, external_id, name) VALUES (:id, :bid, :ext, :name)"
        ),
        {"id": d_id, "bid": b_id, "ext": "dev-3", "name": "Device 3"},
    )

    base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    series = [
        (base_ts, 0.0),
        (base_ts + timedelta(minutes=10), 20.0),
    ]
    for ts, v in series:
        await session.execute(
            text(
                """
                INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
                VALUES (:did, :ts, :metric, :val, :unit, 1)
                """
            ),
            {
                "did": d_id,
                "ts": ts.isoformat(),
                "metric": "energy_kwh_total",
                "val": v,
                "unit": "kWh",
            },
        )

    await session.execute(
        text(
            """
            INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
            VALUES (:did, :ts, :metric, :val, :unit, 1)
            """
        ),
        {
            "did": d_id,
            "ts": (base_ts + timedelta(minutes=5)).isoformat(),
            "metric": "energy_kwh_total",
            "val": 10.0,
            "unit": "kWh",
        },
    )
    await session.commit()

    await session.execute(
        text((_SQL_ROOT / "recompute_energy_deltas.sql").read_text()),
        {"device_id": d_id, "metric": "energy_kwh_total"},
    )
    await session.commit()

    rows = (
        await session.execute(
            text(
                """
                SELECT ts, value, delta
                FROM measurements
                WHERE device_id = :did AND metric = 'energy_kwh_total'
                ORDER BY ts ASC
                """
            ),
            {"did": d_id},
        )
    ).mappings().all()

    values = [float(r["value"]) for r in rows]
    deltas = [r["delta"] for r in rows]

    assert values == [0.0, 10.0, 20.0]
    assert deltas[0] is None
    assert float(deltas[1]) == pytest.approx(10.0)
    assert float(deltas[2]) == pytest.approx(10.0)
