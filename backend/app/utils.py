"""
Shared helpers for the CenEMS API, grouped by functionality into classes.
"""
import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Measurement
from .sql_loader import load_sql

# ---------------------------------------------------------------------------
# Known energy units (case-insensitive)
# ---------------------------------------------------------------------------
ENERGY_UNITS_WH = frozenset({"wh"})
ENERGY_UNITS_KWH = frozenset({"kwh"})


# ---------------------------------------------------------------------------
# Parsing: query and request parameters
# ---------------------------------------------------------------------------
class Parsing:
    """Parse and validate query/request parameters."""

    @staticmethod
    def parse_exclude_bad(value: str) -> bool:
        """
        Parse exclude_bad query parameter.
        Returns True to exclude bad records (filter to is_bad=0),
        False to include them.
        """
        if not value or not isinstance(value, str):
            return True
        v = value.strip().lower()
        if v in ("0", "false", "no"):
            return False
        # Explicit true/1/yes -> exclude;
        # any other value (unknown) also defaults to exclude
        return True


# ---------------------------------------------------------------------------
# Metric normalization: canonical metric/unit and value conversion
# ---------------------------------------------------------------------------
class MetricNorm:
    """Normalize energy metric and unit; convert values to canonical unit."""

    @staticmethod
    def canonical_metric_and_unit(
        metric: str,
        unit: str
    ) -> Tuple[str, str, bool, bool]:
        """
        Normalize metric and unit for energy readings.
        Returns (canonical_metric, canonical_unit, is_normal, is_bad).
        - is_normal: True when unit was converted (e.g. Wh->kWh).
        - is_bad: True when unit is unknown for energy (e.g. kals).
        """
        m = metric.lower().strip()
        u = unit.lower().strip()
        if m in {"energy", "energy_total", "energy_kwh_total"}:
            if u in ENERGY_UNITS_KWH:
                return "energy_kwh_total", "kWh", 0, 0
            if u in ENERGY_UNITS_WH:
                return "energy_kwh_total", "kWh", 1, 0
            return "energy_kwh_total", unit, 0, 1
        return m, u, 0, 0

    @staticmethod
    def convert_value(metric: str, unit: str, value: float) -> float:
        """Convert value to canonical unit (e.g. Wh -> kWh)."""
        m = metric.lower().strip()
        u = unit.lower().strip()
        if m in {"energy", "energy_total", "energy_kwh_total"}:
            if u in ENERGY_UNITS_WH:
                return value / 1000.0
            if u in ENERGY_UNITS_KWH:
                return value
        return value

    @staticmethod
    def _ts_to_iso_utc(ts: str) -> str:
        """
        Format SQLite datetime (UTC, no TZ) as ISO
        with Z so frontend parses as UTC.
        """
        if not ts or "Z" in ts or "+" in ts:
            return ts
        return ts.replace(" ", "T", 1) + "Z"

    @staticmethod
    def build_metric_condition(metric: str, include_legacy_bad: bool) -> str:
        """
        Build SQL fragment for metric filter. When include_legacy_bad and
        metric is energy_kwh_total, includes legacy rows
        (metric='energy' AND is_bad=1).
        """
        if metric == "energy_kwh_total" and include_legacy_bad:
            return "(metric = :metric OR (metric = 'energy' AND is_bad = 1))"
        return "metric = :metric"


# ---------------------------------------------------------------------------
# Ingest: dedupe and keys
# ---------------------------------------------------------------------------
class IngestUtils:
    """Helpers used during ingest (dedupe keys, etc.)."""

    @staticmethod
    def compute_dedupe_key(
        device_external_id: str,
        metric: str,
        ts: datetime,
        value: float,
        explicit_key: Optional[str],
    ) -> str:
        """
        Compute deterministic dedupe key for a reading;
        use explicit_key if provided.
        """
        if explicit_key:
            return explicit_key
        raw = f"{device_external_id}|{metric}|{ts.isoformat()}|{value}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    @staticmethod
    async def detect_latecomer(
        session: AsyncSession,
        device_id: str,
        canonical_metric: str,
        raw_timestamp: datetime,
    ) -> int:
        """
        Returns 1 if ts_record < max_db_ts for device_id else 0
        """
        max_ts_row = (
            await session.execute(
                text(load_sql("measurements_max_ts.sql")),
                {
                    "device_id": device_id,
                    "metric": canonical_metric,
                    "ts": raw_timestamp.isoformat()
                },
            )
        ).mappings().first()
        max_ts = max_ts_row["max_ts"] if max_ts_row is not None else None
        max_ts_parsed = (
            datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
            if max_ts else None
        )
        return 1 if max_ts_parsed and raw_timestamp < max_ts_parsed else 0

    @staticmethod
    async def calculate_delta_energy(
        session: AsyncSession,
        device_id: str,
        canonical_metric: str,
        canonical_value: float,
        raw_timestamp: datetime,
    ) -> float:
        """
        Calculate delta = value[i]-value[i-1].
        If delta < 0, set delta = 0.0.
        """
        previous_row = (
            await session.execute(
                text(load_sql("measurements_latest_ts.sql")),
                {
                    "device_id": device_id,
                    "metric": canonical_metric,
                    "ts": raw_timestamp.isoformat()
                },
            )
        ).mappings().first()
        if previous_row:
            latest_value = previous_row["value"]
            delta = canonical_value - latest_value
            delta = 0.0 if delta < 0 else delta
            return delta
        else:
            return None

    @staticmethod
    async def process_latecomer(
        session: AsyncSession,
        device_id: str,
        canonical_metric: str,
        canonical_value: float,
        raw_timestamp: datetime,
    ) -> None:
        """ Re-calculate delta for value[i+1]; update the record """
        next_row = (
            await session.execute(
                text(load_sql("measurement_next_by_ts.sql")),
                {
                    "device_id": device_id,
                    "metric": canonical_metric,
                    "ts": raw_timestamp.isoformat()
                },
            )
        ).mappings().first()

        if next_row is not None:
            updated_delta = float(next_row["value"]) - canonical_value
            updated_delta = 0.0 if updated_delta < 0 else updated_delta
            updated_reset = 1 if updated_delta == 0 else 0
            await session.execute(
                text(load_sql("measurement_update_delta.sql")),
                {
                    "id": next_row["id"],
                    "delta": updated_delta,
                    "is_reset": updated_reset
                },
            )
        return

# ---------------------------------------------------------------------------
# DB resolution: get-or-create building/device
# ---------------------------------------------------------------------------


class DbResolver:
    """Resolve or create building/device by name or external_id."""

    @staticmethod
    async def get_or_create_building(session: AsyncSession, name: str) -> str:
        """Resolve building by name; create and return id if not found."""
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

    @staticmethod
    async def get_or_create_device(
        session: AsyncSession,
        building_id: str,
        external_id: str,
        name: Optional[str],
    ) -> str:
        """Resolve device by external_id; create and return id if not found."""
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
            {
                "id": device_id,
                "building_id": building_id,
                "external_id": external_id,
                "name": name
            },
        )
        return device_id


# ---------------------------------------------------------------------------
# Query building: filter clauses for aggregated and time-range queries
# ---------------------------------------------------------------------------
class FilterBuilder:
    """
    Build SQL filter clauses and params for
    timeseries/aggregated/sum_deltas.
    """
    @staticmethod
    def build_aggregated_filter(
        exclude_bad: bool,
        start: Optional[datetime],
        end: Optional[datetime],
        table_alias: str = "m",
        metric: Optional[str] = None,
        include_legacy_bad_energy: bool = False,
    ) -> Tuple[str, dict]:
        """
        Build filter clause and params for aggregated/sum_deltas queries.
        Returns (filter_clause, params).
        """
        prefix = table_alias + "." if table_alias else ""
        filter_parts = []
        params = {}
        sql_filters_str = f"""
            AND ({prefix}metric = :metric
            OR ({prefix}metric = 'energy'
            AND {prefix}is_bad = 1))
        """
        if metric is not None:
            if include_legacy_bad_energy:
                filter_parts.append(
                    sql_filters_str
                )
            else:
                filter_parts.append(f"AND {prefix}metric = :metric")
            params["metric"] = metric
        if exclude_bad:
            filter_parts.append(f"AND {prefix}is_bad = 0")
        if start is not None:
            filter_parts.append(f"AND {prefix}ts >= :start")
            params["start"] = start.isoformat()
        if end is not None:
            filter_parts.append(f"AND {prefix}ts <= :end")
            params["end"] = end.isoformat()
        return " ".join(filter_parts), params

    @staticmethod
    def build_time_filter(
        start: Optional[datetime],
        end: Optional[datetime],
        table_alias: str = "m",
    ) -> Tuple[str, dict]:
        """
        Build time-range filter clause and params (e.g. for bad_points query).
        Returns (time_filter, params).
        """
        prefix = table_alias + "." if table_alias else ""
        parts = []
        params = {}
        if start is not None:
            parts.append(f"AND {prefix}ts >= :start")
            params["start"] = start.isoformat()
        if end is not None:
            parts.append(f"AND {prefix}ts <= :end")
            params["end"] = end.isoformat()
        return " ".join(parts), params


# ---------------------------------------------------------------------------
# Mappers: DB row -> Pydantic/model
# ---------------------------------------------------------------------------
class Mappers:
    """Map database rows to API models."""

    @staticmethod
    def row_to_measurement(r: Any) -> Measurement:
        """Map a DB row (mappings result) to a Measurement model."""
        ts_val = r["ts"]
        if isinstance(ts_val, str):
            ts_val = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            if ts_val.tzinfo is None:
                ts_val = ts_val.replace(tzinfo=timezone.utc)
        return Measurement(
            id=int(r["id"]) if r.get("id") is not None else None,
            ts=ts_val,
            metric=r["metric"],
            value=float(r["value"]),
            unit=r["unit"],
            delta=float(r["delta"]) if r.get("delta") is not None else None,
            is_normal=bool(r.get("is_normal", 0)),
            is_reset=bool(r.get("is_reset", 0)),
            is_duplicate=bool(r.get("is_duplicate", 0)),
            is_late=bool(r.get("is_late", 0)),
            is_bad=bool(r.get("is_bad", 0)),
            raw_event_id=(
                int(r["raw_event_id"]) if r.get("raw_event_id") is not None
                else None
            )
        )
