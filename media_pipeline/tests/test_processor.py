from datetime import datetime, timezone

from media_pipeline.pipeline.constants import (
    REASON_UNKNOWN_EVENT_TYPE,
    REASON_VALIDATION_FAILURE,
)
from media_pipeline.pipeline.consumer import MockEventReader
from media_pipeline.pipeline.models import FeatureRecord
from media_pipeline.pipeline.processor import EventProcessor, ProcessingResult
from media_pipeline.pipeline.session import DeadLetterQueue, SessionManager
from media_pipeline.pipeline.writer import EventWriter


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


def test_validation_failure_sent_to_dlq(frozen_time, play_event_payload):
    payload = play_event_payload.copy()
    del payload["user_id"]
    reader = MockEventReader([payload])
    dlq = ListDLQ()
    writer = CapturingWriter()
    processor = EventProcessor(
        reader, SessionManager(dlq), writer, dlq
    )
    result = processor.process_batch()
    assert result.processed == 1
    assert result.dead_lettered == 1
    assert result.features_emitted == 0
    assert dlq.count() == 1
    assert dlq.get_all()[0]["reason"] == REASON_VALIDATION_FAILURE
    assert dlq.get_all()[0]["raw"] == payload


def test_unknown_event_sent_to_dlq(frozen_time, unknown_event_payload):
    reader = MockEventReader([unknown_event_payload])
    dlq = ListDLQ()
    writer = CapturingWriter()
    processor = EventProcessor(
        reader, SessionManager(dlq), writer, dlq
    )
    result = processor.process_batch()
    assert result.processed == 1
    assert result.dead_lettered == 1
    assert result.features_emitted == 0
    assert dlq.count() == 1
    assert dlq.get_all()[0]["reason"] == REASON_UNKNOWN_EVENT_TYPE
    assert "raw_payload" in dlq.get_all()[0]["raw"]


def test_typed_event_play_stop_writes_record(
    frozen_time, play_event_payload, stop_event_payload
):
    reader = MockEventReader([play_event_payload, stop_event_payload])
    dlq = ListDLQ()
    writer = CapturingWriter()
    processor = EventProcessor(
        reader, SessionManager(dlq), writer, dlq
    )
    result = processor.process_batch()
    assert result.processed == 2
    assert result.dead_lettered == 0
    assert result.features_emitted == 1
    assert len(writer.records) == 1
    assert writer.records[0].event_count == 2


def test_processing_result_counts_accurate(
    frozen_time,
    play_event_payload,
    stop_event_payload,
    unknown_event_payload,
):
    invalid = play_event_payload.copy()
    del invalid["user_id"]
    reader = MockEventReader(
        [play_event_payload, stop_event_payload, unknown_event_payload, invalid]
    )
    dlq = ListDLQ()
    writer = CapturingWriter()
    processor = EventProcessor(
        reader, SessionManager(dlq), writer, dlq
    )
    result = processor.process_batch()
    typed_event_count = 2
    assert result.processed == 4
    assert result.dead_lettered == 2
    assert result.processed == result.dead_lettered + typed_event_count
    assert result.features_emitted <= typed_event_count
    assert result.features_emitted >= 0


def test_mark_complete_after_batch(frozen_time, play_event_payload):
    reader = MockEventReader([play_event_payload])
    processor = EventProcessor(
        reader, SessionManager(ListDLQ()), CapturingWriter(), ListDLQ()
    )
    processor.process_batch()
    assert reader.batches_marked_complete == 1


def test_shutdown_flushes_and_closes(frozen_time, play_event_payload):
    reader = MockEventReader([play_event_payload])
    dlq = ListDLQ()
    writer = CapturingWriter()
    processor = EventProcessor(
        reader, SessionManager(dlq), writer, dlq
    )
    processor.process_batch()
    processor.shutdown()
    assert len(writer.records) == 1
    assert writer.records[0].completion_rate is None
    assert writer.records[0].total_watch_seconds == 0.0
    assert reader.batches_marked_complete == 2
    assert reader.is_empty()
    assert reader.read() == []
