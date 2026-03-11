from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Building(BaseModel):
    id: str
    name: str


class Device(BaseModel):
    id: str
    building_id: str
    external_id: str
    name: Optional[str] = None


class Measurement(BaseModel):
    id: Optional[int] = None
    building_id: Optional[str] = None
    building_name: Optional[str] = None
    device_id: Optional[str] = None
    device_external_id: Optional[str] = None
    device_name: Optional[str] = None
    ts: datetime
    metric: str
    value: float
    unit: str
    delta: Optional[float] = None
    is_normal: bool = False
    is_reset: bool = False
    is_duplicate: bool = False
    is_late: bool = False
    is_bad: bool = False
    raw_event_id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class IngestReading(BaseModel):
    timestamp: datetime = Field(..., description="Device timestamp (any timezone, will be stored as UTC)")
    metric: str
    value: float
    unit: str
    dedupe_key: Optional[str] = Field(
        default=None,
        description="Optional deterministic dedupe key; if omitted, the service will compute one.",
    )
    raw_payload: Optional[Dict[str, Any]] = None


class IngestDevice(BaseModel):
    external_id: str
    name: Optional[str] = None


class IngestBuilding(BaseModel):
    name: str


class IngestRequest(BaseModel):
    building: IngestBuilding
    device: IngestDevice
    readings: List[IngestReading]


class HealthResponse(BaseModel):
    status: str
