from typing import Any

import structlog
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

logger = structlog.get_logger()


type UnknownEventPayload = dict[str, Any]


class BaseEvent(BaseModel):
    event_id: str
    event_type: str
    user_id: str
    content_id: str
    timestamp: AwareDatetime
    schema_version: str = Field(default="1.0")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    @field_validator("schema_version")
    @classmethod
    def validate_schema_version(cls, v: str) -> str:
        if v != "1.0":
            logger.warning("Unknown schema version: %s (expected 1.0)", v)
        return v


class PlayEvent(BaseEvent):
    position_seconds: float
    playback_rate: float = 1.0
    subtitle_language: str | None = None


class PauseEvent(BaseEvent):
    position_seconds: float
    buffer_health: float | None = None


class StopEvent(BaseEvent):
    position_seconds: float
    watch_duration_seconds: float
    completion_rate: float | None = None


class UnknownEvent(BaseEvent):
    raw_payload: UnknownEventPayload = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    @classmethod
    def _prepare_unknown_event_data(cls, data: object) -> object:
        if isinstance(data, dict):
            return {**data, "raw_payload": dict(data)}
        return data

    @model_validator(mode="before")
    def capture_raw_payload(cls, data: object) -> object:
        return cls._prepare_unknown_event_data(data)


def parse_event(raw_payload: dict) -> BaseEvent | None:
    event_type = raw_payload.get("event_type")
    try:
        if event_type == "PLAY":
            return PlayEvent(**raw_payload)
        if event_type == "PAUSE":
            return PauseEvent(**raw_payload)
        if event_type == "STOP":
            return StopEvent(**raw_payload)
        if event_type not in ["PLAY", "PAUSE", "STOP"]:
            return UnknownEvent(**raw_payload)
    except ValidationError:
        return None


class FeatureRecord(BaseModel):
    user_id: str
    content_id: str
    session_start: AwareDatetime
    session_end: AwareDatetime
    total_watch_seconds: float
    pause_count: int
    completion_rate: float | None = None
    event_count: int
    schema_version: str = Field(default="1.0")
