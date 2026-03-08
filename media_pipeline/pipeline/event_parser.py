from pydantic import ValidationError

from media_pipeline.pipeline.models import (
    BaseEvent,
    PauseEvent,
    PlayEvent,
    StopEvent,
    UnknownEvent,
)


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
