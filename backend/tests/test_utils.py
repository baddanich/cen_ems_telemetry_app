"""
Unit tests for backend.app.utils (Parsing, MetricNorm, IngestUtils, FilterBuilder, Mappers, DbResolver).
"""
from datetime import datetime, timezone

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.utils import (
    ENERGY_UNITS_KWH,
    ENERGY_UNITS_WH,
    DbResolver,
    FilterBuilder,
    IngestUtils,
    Mappers,
    MetricNorm,
    Parsing,
)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
class TestParsing:
    def test_parse_exclude_bad_true(self):
        assert Parsing.parse_exclude_bad("true") is True
        assert Parsing.parse_exclude_bad("True") is True
        assert Parsing.parse_exclude_bad("1") is True
        assert Parsing.parse_exclude_bad("yes") is True

    def test_parse_exclude_bad_false(self):
        assert Parsing.parse_exclude_bad("false") is False
        assert Parsing.parse_exclude_bad("False") is False
        assert Parsing.parse_exclude_bad("0") is False
        assert Parsing.parse_exclude_bad("no") is False

    def test_parse_exclude_bad_empty_or_none(self):
        assert Parsing.parse_exclude_bad("") is True
        assert Parsing.parse_exclude_bad(None) is True  # type: ignore

    def test_parse_exclude_bad_unknown_defaults_exclude(self):
        assert Parsing.parse_exclude_bad("anything") is True


# ---------------------------------------------------------------------------
# MetricNorm
# ---------------------------------------------------------------------------
class TestMetricNorm:
    def test_canonical_metric_kwh(self):
        m, u, is_normal, is_bad = MetricNorm.canonical_metric_and_unit("energy", "kWh")
        assert m == "energy_kwh_total"
        assert u == "kWh"
        assert is_normal is False
        assert is_bad is False

    def test_canonical_metric_wh_converted(self):
        m, u, is_normal, is_bad = MetricNorm.canonical_metric_and_unit("energy_total", "Wh")
        assert m == "energy_kwh_total"
        assert u == "kWh"
        assert is_normal is True
        assert is_bad is False

    def test_canonical_metric_unknown_unit_is_bad(self):
        m, u, is_normal, is_bad = MetricNorm.canonical_metric_and_unit("energy", "kals")
        assert m == "energy_kwh_total"
        assert u == "kals"
        assert is_bad is True

    def test_canonical_metric_other_metric_passthrough(self):
        m, u, is_normal, is_bad = MetricNorm.canonical_metric_and_unit("temperature", "C")
        assert m == "temperature"
        assert u == "c"  # non-energy: unit is lowercased for consistency
        assert is_bad is False

    def test_convert_value_kwh(self):
        assert MetricNorm.convert_value("energy", "kWh", 10.0) == 10.0

    def test_convert_value_wh_to_kwh(self):
        assert MetricNorm.convert_value("energy", "Wh", 1000.0) == 1.0

    def test_convert_value_other_unchanged(self):
        assert MetricNorm.convert_value("temperature", "C", 25.0) == 25.0

    def test_build_metric_condition_legacy_bad(self):
        s = MetricNorm.build_metric_condition("energy_kwh_total", include_legacy_bad=True)
        assert "metric = :metric" in s
        assert "energy" in s
        assert "is_bad" in s

    def test_build_metric_condition_simple(self):
        s = MetricNorm.build_metric_condition("energy_kwh_total", include_legacy_bad=False)
        assert s == "metric = :metric"
        assert MetricNorm.build_metric_condition("other", True) == "metric = :metric"


# ---------------------------------------------------------------------------
# IngestUtils
# ---------------------------------------------------------------------------
class TestIngestUtils:
    def test_compute_dedupe_key_explicit(self):
        key = IngestUtils.compute_dedupe_key("dev", "m", datetime.now(timezone.utc), 1.0, "my-key")
        assert key == "my-key"

    def test_compute_dedupe_key_deterministic(self):
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        k1 = IngestUtils.compute_dedupe_key("d", "m", ts, 5.0, None)
        k2 = IngestUtils.compute_dedupe_key("d", "m", ts, 5.0, None)
        assert k1 == k2
        assert len(k1) == 64  # sha256 hex

    def test_compute_dedupe_key_different_input_different_key(self):
        ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        k1 = IngestUtils.compute_dedupe_key("d", "m", ts, 5.0, None)
        k2 = IngestUtils.compute_dedupe_key("d", "m", ts, 6.0, None)
        assert k1 != k2


# ---------------------------------------------------------------------------
# FilterBuilder
# ---------------------------------------------------------------------------
class TestFilterBuilder:
    def test_build_aggregated_filter_metric_only(self):
        clause, params = FilterBuilder.build_aggregated_filter(
            True, None, None, "m", metric="energy_kwh_total", include_legacy_bad_energy=False
        )
        assert "metric = :metric" in clause
        assert "is_bad = 0" in clause
        assert params["metric"] == "energy_kwh_total"
        assert "start" not in params
        assert "end" not in params

    def test_build_aggregated_filter_with_time(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)
        clause, params = FilterBuilder.build_aggregated_filter(
            True, start, end, "m", metric="energy_kwh_total", include_legacy_bad_energy=False
        )
        assert "ts >= :start" in clause
        assert "ts <= :end" in clause
        assert "start" in params
        assert "end" in params

    def test_build_aggregated_filter_legacy_bad(self):
        clause, params = FilterBuilder.build_aggregated_filter(
            False, None, None, "m", metric="energy_kwh_total", include_legacy_bad_energy=True
        )
        assert "energy" in clause
        assert "is_bad = 1" in clause

    def test_build_aggregated_filter_empty_prefix(self):
        clause, params = FilterBuilder.build_aggregated_filter(
            True, None, None, "", metric="x", include_legacy_bad_energy=False
        )
        assert "metric = :metric" in clause

    def test_build_time_filter_none(self):
        clause, params = FilterBuilder.build_time_filter(None, None, "m")
        assert clause == ""
        assert params == {}

    def test_build_time_filter_start_end(self):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 2, tzinfo=timezone.utc)
        clause, params = FilterBuilder.build_time_filter(start, end, "m")
        assert "m.ts >= :start" in clause
        assert "m.ts <= :end" in clause
        assert params["start"] == start.isoformat()
        assert params["end"] == end.isoformat()


# ---------------------------------------------------------------------------
# Mappers
# ---------------------------------------------------------------------------
class TestMappers:
    def test_row_to_measurement_minimal(self):
        row = {
            "id": 1,
            "ts": "2024-06-01T12:00:00Z",
            "metric": "energy_kwh_total",
            "value": 10.5,
            "unit": "kWh",
            "delta": 2.0,
            "is_normal": 0,
            "is_reset": 0,
            "is_duplicate": 0,
            "is_late": 0,
            "is_bad": 0,
        }
        m = Mappers.row_to_measurement(row)
        assert m.id == 1
        assert m.metric == "energy_kwh_total"
        assert m.value == 10.5
        assert m.delta == 2.0
        assert m.is_bad is False

    def test_row_to_measurement_ts_datetime(self):
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        row = {
            "id": None,
            "ts": ts,
            "metric": "x",
            "value": 0.0,
            "unit": "u",
            "delta": None,
            "is_normal": 1,
            "is_reset": 1,
            "is_duplicate": 0,
            "is_late": 0,
            "is_bad": 1,
        }
        m = Mappers.row_to_measurement(row)
        assert m.id is None
        assert m.ts == ts
        assert m.delta is None
        assert m.is_normal is True
        assert m.is_reset is True
        assert m.is_bad is True

    def test_row_to_measurement_missing_optionals(self):
        row = {
            "ts": "2024-06-01T12:00:00+00:00",
            "metric": "m",
            "value": 1.0,
            "unit": "u",
        }
        m = Mappers.row_to_measurement(row)
        assert m.id is None
        assert m.delta is None
        assert m.is_normal is False


# ---------------------------------------------------------------------------
# DbResolver (integration-style: needs DB and SQL files)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
class TestDbResolver:
    async def test_get_or_create_building_creates(self, db_pool: AsyncSession) -> None:
        bid = await DbResolver.get_or_create_building(db_pool, "Test Building A")
        assert bid is not None
        row = (await db_pool.execute(text("SELECT id, name FROM buildings WHERE id = :id"), {"id": bid})).mappings().first()
        assert row is not None
        assert row["name"] == "Test Building A"

    async def test_get_or_create_building_returns_existing(self, db_pool: AsyncSession) -> None:
        bid1 = await DbResolver.get_or_create_building(db_pool, "Same Name")
        bid2 = await DbResolver.get_or_create_building(db_pool, "Same Name")
        assert bid1 == bid2

    async def test_get_or_create_device_creates(self, db_pool: AsyncSession) -> None:
        bid = await DbResolver.get_or_create_building(db_pool, "For Device")
        did = await DbResolver.get_or_create_device(db_pool, bid, "ext-1", "Device One")
        assert did is not None
        row = (await db_pool.execute(text("SELECT id, building_id, external_id FROM devices WHERE id = :id"), {"id": did})).mappings().first()
        assert row is not None
        assert row["building_id"] == bid
        assert row["external_id"] == "ext-1"

    async def test_get_or_create_device_returns_existing(self, db_pool: AsyncSession) -> None:
        bid = await DbResolver.get_or_create_building(db_pool, "For Device Idempotent")
        did1 = await DbResolver.get_or_create_device(db_pool, bid, "ext-same", None)
        did2 = await DbResolver.get_or_create_device(db_pool, bid, "ext-same", "Other Name")
        assert did1 == did2
