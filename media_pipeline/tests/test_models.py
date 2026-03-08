from datetime import datetime, timezone

from media_pipeline.pipeline.event_parser import parse_event
from media_pipeline.pipeline.models import (
    FeatureRecord,
    PauseEvent,
    PlayEvent,
    StopEvent,
    UnknownEvent,
    UnknownEventPayload,
)


def test_play_event_is_valid(play_event_payload):
    event = PlayEvent(**play_event_payload)
    assert event.user_id == play_event_payload["user_id"]
    assert event.content_id == play_event_payload["content_id"]
    assert event.position_seconds == play_event_payload["position_seconds"]


def test_play_event_unknown_field_is_ignored(play_event_payload):
    event = PlayEvent(**{**play_event_payload, "unknown_field": "unknown_value"})
    assert event.user_id == play_event_payload["user_id"]
    assert getattr(event, "unknown_field", None) is None


def test_play_event_optional_field_sets_default(play_event_payload):
    event = PlayEvent(**play_event_payload)
    assert event.subtitle_language is None


def test_pause_event_is_valid(pause_event_payload):
    event = PauseEvent(**pause_event_payload)
    assert event.user_id == pause_event_payload["user_id"]
    assert event.content_id == pause_event_payload["content_id"]
    assert event.position_seconds == pause_event_payload["position_seconds"]


def test_pause_event_unknown_field_is_ignored(pause_event_payload):
    event = PauseEvent(**{**pause_event_payload, "unknown_field": "unknown_value"})
    assert event.user_id == pause_event_payload["user_id"]
    assert getattr(event, "unknown_field", None) is None


def test_pause_event_optional_field_sets_default(pause_event_payload):
    event = PauseEvent(**pause_event_payload)
    assert event.buffer_health is None


def test_stop_event_is_valid(stop_event_payload):
    event = StopEvent(**stop_event_payload)
    assert event.user_id == stop_event_payload["user_id"]
    assert event.content_id == stop_event_payload["content_id"]
    assert event.watch_duration_seconds == stop_event_payload["watch_duration_seconds"]


def test_stop_event_unknown_field_is_ignored(stop_event_payload):
    event = StopEvent(**{**stop_event_payload, "unknown_field": "unknown_value"})
    assert event.user_id == stop_event_payload["user_id"]
    assert getattr(event, "unknown_field", None) is None


def test_stop_event_optional_field_sets_default(stop_event_payload):
    event = StopEvent(**stop_event_payload)
    assert event.completion_rate is None


def test_unknown_event_is_valid(unknown_event_payload):
    event = UnknownEvent(**unknown_event_payload)
    assert event.raw_payload == unknown_event_payload


def test_unknown_event_captures_extra_fields_in_raw_payload(unknown_event_payload):
    payload_with_extras: UnknownEventPayload = {
        **unknown_event_payload,
        "unknown_field": "unknown_value",
    }
    event = UnknownEvent(**payload_with_extras)
    assert "unknown_field" in event.raw_payload
    assert event.raw_payload["unknown_field"] == "unknown_value"


def test_unknown_event_raw_payload_accepts_arbitrary_keys(unknown_event_payload):
    payload_with_extras: UnknownEventPayload = {
        **unknown_event_payload,
        "unknown_field": "unknown_value",
        "nested": {"key": "value"},
    }
    event = UnknownEvent(**payload_with_extras)
    assert "unknown_field" in event.raw_payload
    assert "nested" in event.raw_payload
    assert event.raw_payload["nested"] == {"key": "value"}


def test_parse_event_returns_correct_event_type(
    frozen_time,
    play_event_payload,
    pause_event_payload,
    stop_event_payload,
    unknown_event_payload,
):
    assert isinstance(parse_event(play_event_payload), PlayEvent)
    assert isinstance(parse_event(pause_event_payload), PauseEvent)
    assert isinstance(parse_event(stop_event_payload), StopEvent)
    assert isinstance(parse_event(unknown_event_payload), UnknownEvent)


def test_parse_event_missing_required_fields_returns_none(play_event_payload):
    payload = play_event_payload.copy()
    del payload["user_id"]
    assert parse_event(payload) is None


def test_parse_event_invalid_timestamp_returns_none(play_event_payload):
    event = parse_event({**play_event_payload, "timestamp": "invalid_timestamp"})
    assert event is None


def test_parse_event_unknown_type_invalid_payload_returns_none(unknown_event_payload):
    payload = unknown_event_payload.copy()
    del payload["user_id"]
    assert parse_event(payload) is None


def test_parse_event_empty_payload_returns_none():
    assert parse_event({}) is None


def test_parse_event_invalid_field_type_returns_none(unknown_event_payload):
    payload = {**unknown_event_payload, "content_id": 123}
    assert parse_event(payload) is None


def test_unknown_schema_version_accepted(play_event_payload):
    payload = {**play_event_payload, "schema_version": "2.0"}
    event = PlayEvent(**payload)
    assert event.schema_version == "2.0"
    assert event.user_id == play_event_payload["user_id"]


def test_feature_record_serialises_datetimes():
    feature_record = FeatureRecord(
        user_id="user123",
        content_id="content123",
        session_start=datetime.now(timezone.utc),
        session_end=datetime.now(timezone.utc),
        total_watch_seconds=10.0,
        pause_count=1,
        completion_rate=0.5,
        event_count=1,
    )
    dumped = feature_record.model_dump(mode="json")
    assert isinstance(dumped["session_start"], str)
    assert isinstance(dumped["session_end"], str)
    session_start = datetime.fromisoformat(
        dumped["session_start"].replace("Z", "+00:00")
    )
    session_end = datetime.fromisoformat(dumped["session_end"].replace("Z", "+00:00"))
    assert session_start == feature_record.session_start
    assert session_end == feature_record.session_end
