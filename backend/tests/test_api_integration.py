from datetime import datetime, timezone, timedelta

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.main import create_app


@pytest.mark.asyncio
async def test_health_endpoint(db_pool: AsyncSession, ensure_db_connected) -> None:
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_ingest_and_query_latest_and_timeseries(db_pool: AsyncSession, ensure_db_connected) -> None:
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        base_ts = datetime.now(timezone.utc).replace(microsecond=0)
        payload = {
            "building": {"name": "Building A"},
            "device": {"external_id": "meter-1", "name": "Main meter"},
            "readings": [
                {
                    "timestamp": (base_ts + timedelta(minutes=0)).isoformat(),
                    "metric": "energy",
                    "value": 0.0,
                    "unit": "kWh",
                },
                {
                    "timestamp": (base_ts + timedelta(minutes=5)).isoformat(),
                    "metric": "energy",
                    "value": 10.0,
                    "unit": "kWh",
                },
                {
                    "timestamp": (base_ts + timedelta(minutes=5)).isoformat(),
                    "metric": "energy",
                    "value": 10.0,
                    "unit": "kWh",
                },
            ],
        }

        resp = await client.post("/ingest", json=payload)
        assert resp.status_code == 202

        buildings = (await client.get("/buildings")).json()
        assert len(buildings) == 1
        building_id = buildings[0]["id"]

        devices = (await client.get(f"/buildings/{building_id}/devices")).json()
        assert len(devices) == 1
        device_id = devices[0]["id"]

        latest = (await client.get(f"/devices/{device_id}/latest")).json()
        assert len(latest) == 1
        m = latest[0]
        assert m["metric"] == "energy_kwh_total"
        assert m["unit"] == "kWh"
        assert m["value"] == pytest.approx(10.0)
        assert m["delta"] == pytest.approx(10.0)
        assert m["is_normal"] is False  # kWh given, no conversion
        assert m["is_reset"] is False
        assert m["is_duplicate"] is False
        assert m["is_late"] is False
        assert m["is_bad"] is False

        ts = (
            await client.get(
                "/timeseries",
                params={"device_id": device_id, "metric": "energy_kwh_total"},
            )
        ).json()
        assert len(ts) == 2
        assert ts[0]["delta"] is None
        assert ts[1]["delta"] == pytest.approx(10.0)


@pytest.mark.asyncio
async def test_timeseries_time_range_filter(db_pool: AsyncSession, ensure_db_connected) -> None:
    """Check that start/end datetime filter limits timeseries results."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        base_ts = datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc)
        payload = {
            "building": {"name": "TimeRange Building"},
            "device": {"external_id": "meter-tr", "name": "TR meter"},
            "readings": [
                {"timestamp": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy", "value": 0.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=15)).isoformat(), "metric": "energy", "value": 5.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=30)).isoformat(), "metric": "energy", "value": 12.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=45)).isoformat(), "metric": "energy", "value": 20.0, "unit": "kWh"},
            ],
        }
        resp = await client.post("/ingest", json=payload)
        assert resp.status_code == 202
        buildings = (await client.get("/buildings")).json()
        building_id = buildings[0]["id"]
        devices = (await client.get(f"/buildings/{building_id}/devices")).json()
        device_id = devices[0]["id"]

        start_inclusive = (base_ts + timedelta(minutes=15)).isoformat()
        end_exclusive = (base_ts + timedelta(minutes=35)).isoformat()
        ts = (
            await client.get(
                "/timeseries",
                params={
                    "device_id": device_id,
                    "metric": "energy_kwh_total",
                    "start": start_inclusive,
                    "end": end_exclusive,
                },
            )
        ).json()
        assert len(ts) == 2
        assert ts[0]["value"] == pytest.approx(5.0)
        assert ts[1]["value"] == pytest.approx(12.0)


@pytest.mark.asyncio
async def test_recent_exclude_bad(db_pool: AsyncSession, ensure_db_connected) -> None:
    """With exclude_bad=false, recent returns records including is_bad=1; with exclude_bad=true they are filtered out."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        base_ts = datetime.now(timezone.utc).replace(microsecond=0)
        # Ingest one good (kWh) and one bad (kals) reading so both are stored under energy_kwh_total
        payload = {
            "building": {"name": "BadRecords Building"},
            "device": {"external_id": "meter-bad", "name": "Bad meter"},
            "readings": [
                {"timestamp": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy", "value": 0.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=1)).isoformat(), "metric": "energy", "value": 100.0, "unit": "kals"},
            ],
        }
        resp = await client.post("/ingest", json=payload)
        assert resp.status_code == 202
        buildings = (await client.get("/buildings")).json()
        building = next((b for b in buildings if b.get("name") == "BadRecords Building"), buildings[0])
        building_id = building["id"]
        devices = (await client.get(f"/buildings/{building_id}/devices")).json()
        device_id = next((d["id"] for d in devices if d.get("external_id") == "meter-bad"), devices[0]["id"])

        # Include bad: should get 2 records, one with is_bad true
        recent_include = (
            await client.get(
                f"/devices/{device_id}/recent",
                params={"metric": "energy_kwh_total", "limit": 10, "offset": 0, "exclude_bad": "false"},
            )
        ).json()
        assert len(recent_include) == 2
        bad_records = [m for m in recent_include if m.get("is_bad")]
        assert len(bad_records) == 1
        assert bad_records[0]["unit"] == "kals" and bad_records[0]["value"] == pytest.approx(100.0)

        # Exclude bad: should get only the good record(s) – one row (first is good)
        recent_exclude = (
            await client.get(
                f"/devices/{device_id}/recent",
                params={"metric": "energy_kwh_total", "limit": 10, "offset": 0, "exclude_bad": "true"},
            )
        ).json()
        assert len(recent_exclude) == 1
        assert recent_exclude[0]["is_bad"] is False
        assert recent_exclude[0]["unit"] == "kWh"
