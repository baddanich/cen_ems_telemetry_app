from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_SQL_ROOT = Path(__file__).resolve().parents[2] / "sql" / "queries"


# @pytest.mark.asyncio
# async def test_out_of_order_inserts_recomputed_correctly(db_pool: AsyncSession) -> None:
#     session = db_pool
#     b_id = "b3"
#     await session.execute(
#         text("INSERT INTO buildings (id, name) VALUES (:id, :name)"),
#         {"id": b_id, "name": "B3"},
#     )
#     d_id = "d3"
#     await session.execute(
#         text(
#             "INSERT INTO devices (id, building_id, external_id, name) VALUES (:id, :bid, :ext, :name)"
#         ),
#         {"id": d_id, "bid": b_id, "ext": "dev-3", "name": "Device 3"},
#     )

#     base_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
#     series = [
#         (base_ts, 0.0),
#         (base_ts + timedelta(minutes=10), 20.0),
#     ]
#     for ts, v in series:
#         await session.execute(
#             text(
#                 """
#                 INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
#                 VALUES (:did, :ts, :metric, :val, :unit, 1)
#                 """
#             ),
#             {
#                 "did": d_id,
#                 "ts": ts.isoformat(),
#                 "metric": "energy_kwh_total",
#                 "val": v,
#                 "unit": "kWh",
#             },
#         )

#     await session.execute(
#         text(
#             """
#             INSERT INTO measurements (device_id, ts, metric, value, unit, is_normal)
#             VALUES (:did, :ts, :metric, :val, :unit, 1)
#             """
#         ),
#         {
#             "did": d_id,
#             "ts": (base_ts + timedelta(minutes=5)).isoformat(),
#             "metric": "energy_kwh_total",
#             "val": 10.0,
#             "unit": "kWh",
#         },
#     )
#     await session.commit()

#     await session.execute(
#         text((_SQL_ROOT / "recompute_energy_deltas.sql").read_text()),
#         {"device_id": d_id, "metric": "energy_kwh_total"},
#     )
#     await session.commit()

#     rows = (
#         await session.execute(
#             text(
#                 """
#                 SELECT ts, value, delta
#                 FROM measurements
#                 WHERE device_id = :did AND metric = 'energy_kwh_total'
#                 ORDER BY ts ASC
#                 """
#             ),
#             {"did": d_id},
#         )
#     ).mappings().all()

#     values = [float(r["value"]) for r in rows]
#     deltas = [r["delta"] for r in rows]

#     assert values == [0.0, 10.0, 20.0]
#     assert deltas[0] is None
#     assert float(deltas[1]) == pytest.approx(10.0)
#     assert float(deltas[2]) == pytest.approx(10.0)
