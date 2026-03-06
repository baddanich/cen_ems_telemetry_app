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
