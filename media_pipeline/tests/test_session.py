from media_pipeline.pipeline.constants import REASON_LATE_EVENT
from media_pipeline.pipeline.models import (
    FeatureRecord,
    PauseEvent,
    PlayEvent,
    StopEvent,
    parse_event,
)
from media_pipeline.pipeline.session import Session, SessionManager
from media_pipeline.tests.conftest import ListDLQ


def test_session_add_event_play_then_stop_produces_complete_session(
    frozen_time, play_event_payload, stop_event_payload
):
    play = parse_event(play_event_payload)
    stop = parse_event(stop_event_payload)
    assert isinstance(play, PlayEvent)
    assert isinstance(stop, StopEvent)
    session = Session(play.user_id, play.content_id, play.timestamp)
    assert session.add_event(play) is True
    assert session.is_complete() is False
    assert session.add_event(stop) is True
    assert session.is_complete() is True
    record = session.to_feature_record()
    assert isinstance(record, FeatureRecord)
    assert record.user_id == play.user_id
    assert record.event_count == 2
    assert record.total_watch_seconds == stop.watch_duration_seconds


def test_session_duplicate_stop_returns_false(frozen_time, stop_event_payload):
    stop = parse_event(stop_event_payload)
    assert isinstance(stop, StopEvent)
    session = Session(stop.user_id, stop.content_id, stop.timestamp)
    assert session.add_event(stop) is True
    assert session.add_event(stop) is False


def test_session_to_feature_record_incomplete(frozen_time, pause_event_payload):
    pause = parse_event(pause_event_payload)
    assert isinstance(pause, PauseEvent)
    session = Session(pause.user_id, pause.content_id, pause.timestamp)
    session.add_event(pause)
    assert session.is_complete() is False
    record = session.to_feature_record()
    assert record.completion_rate is None
    assert record.pause_count == 1
    assert record.event_count == 1


def test_session_manager_process_event_returns_record_on_stop(
    frozen_time, play_event_payload, stop_event_payload
):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    stop = parse_event(stop_event_payload)
    assert isinstance(play, PlayEvent)
    assert isinstance(stop, StopEvent)
    assert mgr.process_event(play) is None
    record = mgr.process_event(stop)
    assert record is not None
    assert isinstance(record, FeatureRecord)
    assert record.event_count == 2
    assert dlq.count() == 0


def test_session_manager_first_event_stop_returns_record_and_closes_key(
    frozen_time, stop_event_payload
):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    stop = parse_event(stop_event_payload)
    assert isinstance(stop, StopEvent)
    record = mgr.process_event(stop)
    assert record is not None
    assert isinstance(record, FeatureRecord)
    assert record.event_count == 1
    assert record.total_watch_seconds == stop.watch_duration_seconds

    assert record.session_start == stop.timestamp
    assert record.session_end == stop.timestamp

    stop2 = parse_event({**stop_event_payload, "event_id": "456"})
    assert isinstance(stop2, StopEvent)
    assert mgr.process_event(stop2) is None
    assert dlq.count() == 1
    assert dlq.get_all()[0]["reason"] == REASON_LATE_EVENT


def test_session_manager_duplicate_stop_sent_to_dlq(
    frozen_time, play_event_payload, stop_event_payload
):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    stop1 = parse_event(stop_event_payload)
    stop2 = parse_event({**stop_event_payload, "event_id": "456"})
    assert isinstance(play, PlayEvent)
    assert isinstance(stop1, StopEvent)
    assert isinstance(stop2, StopEvent)
    mgr.process_event(play)
    mgr.process_event(stop1)
    assert mgr.process_event(stop2) is None
    assert dlq.count() == 1
    entries = dlq.get_all()

    assert entries[0]["reason"] == REASON_LATE_EVENT
    assert entries[0]["raw"]["event_id"] == "456"


def test_session_manager_late_event_sent_to_dlq(
    frozen_time,
    play_event_payload,
    stop_event_payload,
    pause_event_payload,
):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    stop = parse_event(stop_event_payload)
    pause = parse_event(pause_event_payload)
    assert isinstance(play, PlayEvent)
    assert isinstance(stop, StopEvent)
    assert isinstance(pause, PauseEvent)
    mgr.process_event(play)
    mgr.process_event(stop)
    mgr.process_event(pause)
    assert dlq.count() == 1
    entries = dlq.get_all()
    assert entries[0]["reason"] == REASON_LATE_EVENT
    assert entries[0]["raw"]["event_type"] == "PAUSE"


def test_flush_returns_all_remaining_sessions(
    frozen_time, play_event_payload, pause_event_payload
):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    pause = parse_event(pause_event_payload)
    assert isinstance(play, PlayEvent)
    assert isinstance(pause, PauseEvent)
    mgr.process_event(play)
    mgr.process_event(pause)
    records = mgr.flush()
    assert len(records) == 1
    assert records[0].user_id == play.user_id
    assert records[0].event_count == 2
    assert records[0].completion_rate is None


def test_flush_clears_sessions(frozen_time, play_event_payload):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    assert isinstance(play, PlayEvent)
    mgr.process_event(play)
    records = mgr.flush()
    assert len(records) == 1
    assert mgr.flush() == []


def test_flush_does_not_use_dlq(frozen_time, play_event_payload):
    dlq = ListDLQ()
    mgr = SessionManager(dlq)
    play = parse_event(play_event_payload)
    assert isinstance(play, PlayEvent)
    mgr.process_event(play)
    mgr.flush()
    assert dlq.count() == 0
