from datetime import datetime, timezone

from media_pipeline.pipeline.models import (
    FeatureRecord,
    PauseEvent,
    PlayEvent,
    StopEvent,
    UnknownEvent,
    UnknownEventPayload,
    parse_event,
)


def test_play_event_valid(play_event_payload):
    event = PlayEvent(**play_event_payload)
    assert isinstance(event, PlayEvent)


def test_play_event_unknown_field_ignored(play_event_payload):
    event = PlayEvent(**{**play_event_payload, "unknown_field": "unknown_value"})
    assert isinstance(event, PlayEvent)


def test_play_event_optional_field_default(play_event_payload):
    event = PlayEvent(**play_event_payload)
    assert event.subtitle_language is None


def test_pause_event_valid(pause_event_payload):
    event = PauseEvent(**pause_event_payload)
    assert isinstance(event, PauseEvent)


def test_pause_event_unknown_field_ignored(pause_event_payload):
    event = PauseEvent(**{**pause_event_payload, "unknown_field": "unknown_value"})
    assert isinstance(event, PauseEvent)


def test_pause_event_optional_field_default(pause_event_payload):
    event = PauseEvent(**pause_event_payload)
    assert event.buffer_health is None


def test_stop_event_valid(stop_event_payload):
    event = StopEvent(**stop_event_payload)
    assert isinstance(event, StopEvent)


def test_stop_event_unknown_field_ignored(stop_event_payload):
    event = StopEvent(**{**stop_event_payload, "unknown_field": "unknown_value"})
    assert isinstance(event, StopEvent)


def test_stop_event_optional_field_default(stop_event_payload):
    event = StopEvent(**stop_event_payload)
    assert event.completion_rate is None


def test_unknown_event_valid(unknown_event_payload):
    event = UnknownEvent(**unknown_event_payload)
    assert isinstance(event, UnknownEvent)
    assert event.raw_payload == unknown_event_payload


def test_unknown_event_captures_extra_fields_in_raw_payload(unknown_event_payload):
    payload_with_extras: UnknownEventPayload = {
        **unknown_event_payload,
        "unknown_field": "unknown_value",
    }
    event = UnknownEvent(**payload_with_extras)
    assert isinstance(event, UnknownEvent)
    assert "unknown_field" in event.raw_payload
    assert event.raw_payload["unknown_field"] == "unknown_value"


def test_unknown_event_raw_payload_accepts_arbitrary_keys(unknown_event_payload):
    payload_with_extras: UnknownEventPayload = {
        **unknown_event_payload,
        "unknown_field": "unknown_value",
        "nested": {"key": "value"},
    }
    event = UnknownEvent(**payload_with_extras)
    assert isinstance(event, UnknownEvent)
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
    assert isinstance(event, PlayEvent)
    assert event.schema_version == "2.0"


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
    assert session_start.tzinfo is not None
    assert session_end.tzinfo is not None
