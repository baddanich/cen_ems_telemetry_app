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
async def test_buildings_list(db_pool: AsyncSession, ensure_db_connected) -> None:
    """GET /buildings returns list of {id, name}; empty until ingest creates one."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/buildings")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        for b in data:
            assert "id" in b and "name" in b


@pytest.mark.asyncio
async def test_buildings_devices_list(db_pool: AsyncSession, ensure_db_connected) -> None:
    """GET /buildings/{building_id}/devices returns list of devices for that building."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/ingest",
            json={
                "building": {"name": "DevList Building"},
                "device": {"external_id": "dev-1", "name": "Device 1"},
                "readings": [
                    {"timestamp": "2024-06-01T10:00:00Z", "metric": "energy", "value": 0.0, "unit": "kWh"},
                ],
            },
        )
        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "DevList Building"), buildings[0]["id"])
        resp = await client.get(f"/buildings/{bid}/devices")
        assert resp.status_code == 200
        devices = resp.json()
        assert isinstance(devices, list)
        assert len(devices) >= 1
        d = next((x for x in devices if x.get("external_id") == "dev-1"), devices[0])
        assert d["building_id"] == bid
        assert "id" in d and "external_id" in d and "name" in d


@pytest.mark.asyncio
async def test_devices_all(db_pool: AsyncSession, ensure_db_connected) -> None:
    """GET /devices/all returns latest measurements across all devices."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/ingest",
            json={
                "building": {"name": "AllDev Building"},
                "device": {"external_id": "meter-all", "name": "Meter"},
                "readings": [
                    {"timestamp": "2024-06-01T10:00:00Z", "metric": "energy", "value": 1.0, "unit": "kWh"},
                ],
            },
        )
        resp = await client.get("/devices/all")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        if data:
            for m in data:
                assert "ts" in m and "value" in m and "metric" in m


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


@pytest.mark.asyncio
async def test_duplicate_dedupe_key_sets_is_duplicate(db_pool: AsyncSession, ensure_db_connected) -> None:
    """When the same reading (same dedupe_key) is ingested twice, raw_events marks duplicate and measurement gets is_duplicate=1."""
    app = create_app()
    transport = ASGITransport(app=app)
    base_ts = datetime(2024, 6, 10, 14, 0, 0, tzinfo=timezone.utc)
    reading = {"timestamp": base_ts.isoformat(), "metric": "energy", "value": 7.0, "unit": "kWh"}
    payload = {
        "building": {"name": "Dup Building"},
        "device": {"external_id": "meter-dup", "name": "Dup meter"},
        "readings": [reading],
    }
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/ingest", json=payload)
        assert r1.status_code == 202
        r2 = await client.post("/ingest", json=payload)
        assert r2.status_code == 202

        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "Dup Building"), buildings[0]["id"])
        devices = (await client.get(f"/buildings/{bid}/devices")).json()
        did = next((d["id"] for d in devices if d.get("external_id") == "meter-dup"), devices[0]["id"])

        recent = (
            await client.get(
                f"/devices/{did}/recent",
                params={"metric": "energy_kwh_total", "limit": 10, "exclude_bad": "false"},
            )
        ).json()
        # Expanded: one row per ingest (original + duplicate)
        assert len(recent) == 2
        by_dup = {bool(r["is_duplicate"]): r for r in recent}
        assert by_dup[False]["value"] == pytest.approx(7.0)
        assert by_dup[True]["value"] == pytest.approx(7.0)
        assert by_dup[True]["is_duplicate"] is True
        assert by_dup[False]["is_duplicate"] is False


@pytest.mark.asyncio
async def test_sum_deltas_excludes_bad(db_pool: AsyncSession, ensure_db_connected) -> None:
    """Sum deltas always excludes bad records so the total is not affected."""
    app = create_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        base_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        # Put bad reading last so good deltas are 0->10 (10), 10->20 (10); sum = 20
        payload = {
            "building": {"name": "SumBad Building"},
            "device": {"external_id": "meter-sumbad", "name": "SumBad meter"},
            "readings": [
                {"timestamp": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy", "value": 0.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=15)).isoformat(), "metric": "energy", "value": 10.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=30)).isoformat(), "metric": "energy", "value": 20.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=45)).isoformat(), "metric": "energy", "value": 999.0, "unit": "kals"},
            ],
        }
        resp = await client.post("/ingest", json=payload)
        assert resp.status_code == 202
        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "SumBad Building"), buildings[0]["id"])
        devices = (await client.get(f"/buildings/{bid}/devices")).json()
        did = next((d["id"] for d in devices if d.get("external_id") == "meter-sumbad"), devices[0]["id"])

        start = (base_ts + timedelta(minutes=0)).isoformat()
        end = (base_ts + timedelta(minutes=60)).isoformat()
        r = await client.get(
            "/timeseries/sum_deltas",
            params={"building_id": bid, "device_id": did, "metric": "energy_kwh_total", "start": start, "end": end},
        )
        assert r.status_code == 200
        data = r.json()
        # Good deltas: 0->10 (10), 10->20 (10). Bad (999 kals) must not be included.
        assert data["sum_delta"] == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_late_out_of_order_flag(db_pool: AsyncSession, ensure_db_connected) -> None:
    """
    Two requests: (1) Ingest 10, 20, 25, 30 (15 missing) — show delta/sum.
    (2) Ingest late value 15; assert is_late and corrected deltas.
    """
    app = create_app()
    transport = ASGITransport(app=app)
    base_ts = datetime(2024, 7, 1, 9, 0, 0, tzinfo=timezone.utc)
    start_iso = (base_ts).isoformat()
    end_iso = (base_ts + timedelta(minutes=45)).isoformat()

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # ----- Request 1: ingest 10, 20, 25, 30 (value 15 is missing between 10 and 20)
        payload1 = {
            "building": {"name": "Late Building"},
            "device": {"external_id": "meter-late", "name": "Late meter"},
            "readings": [
                {"timestamp": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy", "value": 10.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=10)).isoformat(), "metric": "energy", "value": 20.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=20)).isoformat(), "metric": "energy", "value": 25.0, "unit": "kWh"},
                {"timestamp": (base_ts + timedelta(minutes=30)).isoformat(), "metric": "energy", "value": 30.0, "unit": "kWh"},
            ],
        }
        resp1 = await client.post("/ingest", json=payload1)
        assert resp1.status_code == 202

        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "Late Building"), buildings[0]["id"])
        devices = (await client.get(f"/buildings/{bid}/devices")).json()
        did = next((d["id"] for d in devices if d.get("external_id") == "meter-late"), devices[0]["id"])

        # Show delta: timeseries (asc) gives deltas 10, 5, 5 (missing 15 → one big step 10)
        ts1 = await client.get(
            "/timeseries",
            params={"device_id": did, "metric": "energy_kwh_total", "start": start_iso, "end": end_iso, "exclude_bad": "false"},
        )
        assert ts1.status_code == 200
        rows1 = ts1.json()
        assert len(rows1) == 4
        deltas_before = [r["delta"] for r in rows1 if r.get("delta") is not None]
        assert deltas_before == [10.0, 5.0, 5.0]  # 10→20, 20→25, 25→30
        sum1 = (await client.get("/timeseries/sum_deltas", params={"building_id": bid, "device_id": did, "metric": "energy_kwh_total", "start": start_iso, "end": end_iso})).json()
        assert sum1["sum_delta"] == pytest.approx(20.0)

        # ----- Request 2: ingest late value 15 at 09:05 (between 09:00 and 09:10)
        payload2 = {
            "building": {"name": "Late Building"},
            "device": {"external_id": "meter-late", "name": "Late meter"},
            "readings": [
                {"timestamp": (base_ts + timedelta(minutes=5)).isoformat(), "metric": "energy", "value": 15.0, "unit": "kWh"},
            ],
        }
        resp2 = await client.post("/ingest", json=payload2)
        assert resp2.status_code == 202

        # System understood it was late
        recent = (
            await client.get(
                f"/devices/{did}/recent",
                params={"metric": "energy_kwh_total", "limit": 10, "offset": 0, "exclude_bad": "false"},
            )
        ).json()
        late_row = next((r for r in recent if r.get("value") == 15.0), None)
        assert late_row is not None
        assert late_row["is_late"] is True

        # Corrected delta: timeseries now 10, 15, 20, 25, 30 → deltas 5, 5, 5, 5
        ts2 = await client.get(
            "/timeseries",
            params={"device_id": did, "metric": "energy_kwh_total", "start": start_iso, "end": end_iso, "exclude_bad": "false"},
        )
        assert ts2.status_code == 200
        rows2 = ts2.json()
        assert len(rows2) == 5
        deltas_after = [r["delta"] for r in rows2 if r.get("delta") is not None]
        assert deltas_after == [5.0, 5.0, 5.0, 5.0]  # corrected
        sum2 = (await client.get("/timeseries/sum_deltas", params={"building_id": bid, "device_id": did, "metric": "energy_kwh_total", "start": start_iso, "end": end_iso})).json()
        assert sum2["sum_delta"] == pytest.approx(20.0)


@pytest.mark.asyncio
async def test_timeseries_aggregated(db_pool: AsyncSession, ensure_db_connected) -> None:
    """GET /timeseries/aggregated: building_id=all (per-building series) and building_id=X, device_id=all (Total)."""
    app = create_app()
    transport = ASGITransport(app=app)
    base_ts = datetime(2024, 6, 15, 8, 0, 0, tzinfo=timezone.utc)
    start_iso = base_ts.isoformat()
    end_iso = (base_ts + timedelta(hours=1)).isoformat()
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/ingest",
            json={
                "building": {"name": "Agg Building"},
                "device": {"external_id": "m1", "name": "M1"},
                "readings": [
                    {"timestamp": (base_ts + timedelta(minutes=0)).isoformat(), "metric": "energy", "value": 0.0, "unit": "kWh"},
                    {"timestamp": (base_ts + timedelta(minutes=30)).isoformat(), "metric": "energy", "value": 5.0, "unit": "kWh"},
                ],
            },
        )
        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "Agg Building"), buildings[0]["id"])

        # building_id=all: returns rows with label per building
        r_all = await client.get(
            "/timeseries/aggregated",
            params={"building_id": "all", "metric": "energy_kwh_total", "start": start_iso, "end": end_iso},
        )
        assert r_all.status_code == 200
        agg_all = r_all.json()
        assert isinstance(agg_all, list)
        for row in agg_all:
            assert "ts" in row and "value" in row and "delta" in row and "label" in row

        # building_id=X, device_id=all: one series (Total)
        r_b = await client.get(
            "/timeseries/aggregated",
            params={"building_id": bid, "device_id": "all", "metric": "energy_kwh_total", "start": start_iso, "end": end_iso},
        )
        assert r_b.status_code == 200
        agg_b = r_b.json()
        assert isinstance(agg_b, list)
        for row in agg_b:
            assert row.get("label") == "Total"


@pytest.mark.asyncio
async def test_timeseries_aggregated_bad_points(db_pool: AsyncSession, ensure_db_connected) -> None:
    """GET /timeseries/aggregated_bad_points: only building_id=all returns data; other building_id returns []."""
    app = create_app()
    transport = ASGITransport(app=app)
    base_ts = datetime(2024, 6, 20, 12, 0, 0, tzinfo=timezone.utc)
    start_iso = base_ts.isoformat()
    end_iso = (base_ts + timedelta(hours=1)).isoformat()
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/ingest",
            json={
                "building": {"name": "BadPoints Building"},
                "device": {"external_id": "bp", "name": "BP"},
                "readings": [
                    {"timestamp": base_ts.isoformat(), "metric": "energy", "value": 100.0, "unit": "kals"},
                ],
            },
        )
        buildings = (await client.get("/buildings")).json()
        bid = next((b["id"] for b in buildings if b.get("name") == "BadPoints Building"), buildings[0]["id"])

        r_all = await client.get(
            "/timeseries/aggregated_bad_points",
            params={"building_id": "all", "metric": "energy_kwh_total", "start": start_iso, "end": end_iso},
        )
        assert r_all.status_code == 200
        bad_all = r_all.json()
        assert isinstance(bad_all, list)
        for row in bad_all:
            assert "ts" in row and "label" in row and "value" in row and "delta" in row

        r_one = await client.get(
            "/timeseries/aggregated_bad_points",
            params={"building_id": bid, "metric": "energy_kwh_total", "start": start_iso, "end": end_iso},
        )
        assert r_one.status_code == 200
        assert r_one.json() == []
