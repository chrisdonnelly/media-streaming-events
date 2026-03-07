import pytest
import time_machine
from datetime import datetime, timezone

from media_pipeline.pipeline.models import FeatureRecord
from media_pipeline.pipeline.session import DeadLetterQueue
from media_pipeline.pipeline.writer import EventWriter

FROZEN_TIMESTAMP = "2026-03-07T12:00:00+00:00"


class CapturingWriter(EventWriter):
    def __init__(self):
        self.records: list[FeatureRecord] = []

    def write(self, record: FeatureRecord) -> None:
        self.records.append(record)


class ListDLQ(DeadLetterQueue):
    def __init__(self):
        self._entries: list[dict] = []

    def add(self, raw: dict, reason: str) -> None:
        self._entries.append(
            {"raw": raw, "reason": reason, "timestamp": datetime.now(timezone.utc)}
        )

    def get_all(self) -> list[dict]:
        return list(self._entries)

    def count(self) -> int:
        return len(self._entries)


@pytest.fixture
def frozen_time():
    with time_machine.travel(datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)):
        yield


@pytest.fixture
def base_event_payload():
    return {
        "event_id": "123",
        "user_id": "user123",
        "content_id": "content123",
        "timestamp": FROZEN_TIMESTAMP,
        "schema_version": "1.0",
    }


@pytest.fixture
def play_event_payload(base_event_payload):
    return {**base_event_payload, "event_type": "PLAY", "position_seconds": 10.0}


@pytest.fixture
def pause_event_payload(base_event_payload):
    return {**base_event_payload, "event_type": "PAUSE", "position_seconds": 10.0}


@pytest.fixture
def stop_event_payload(base_event_payload):
    return {
        **base_event_payload,
        "event_type": "STOP",
        "position_seconds": 10.0,
        "watch_duration_seconds": 30.0,
    }


@pytest.fixture
def unknown_event_payload(base_event_payload):
    return {**base_event_payload, "event_type": "SEEK"}
