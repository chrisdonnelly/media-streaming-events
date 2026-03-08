from dataclasses import dataclass

from .constants import (
    REASON_UNKNOWN_EVENT_TYPE,
    REASON_VALIDATION_FAILURE,
)
from .event_parser import parse_event
from .models import UnknownEvent
from .writer import EventWriter


@dataclass
class ProcessingResult:
    processed: int
    dead_lettered: int
    features_emitted: int


class EventProcessor:
    def __init__(
        self,
        reader,
        session_manager,
        writer: EventWriter,
        dead_letter_queue,
    ):
        self._reader = reader
        self._session_manager = session_manager
        self._writer = writer
        self._dead_letter_queue = dead_letter_queue

    def process_batch(self) -> ProcessingResult:
        batch = self._reader.read()
        dead_lettered = 0
        features_emitted = 0
        for raw_event in batch:
            dead_letter, feature = self._process_event(raw_event)
            dead_lettered += dead_letter
            features_emitted += feature
        self._reader.mark_complete()
        return ProcessingResult(
            processed=len(batch),
            dead_lettered=dead_lettered,
            features_emitted=features_emitted,
        )

    def _process_event(self, raw_event: dict) -> tuple[int, int]:
        event = parse_event(raw_payload=raw_event)
        if event is None:
            self._dead_letter_queue.add(raw_event, REASON_VALIDATION_FAILURE)
            return (1, 0)
        if isinstance(event, UnknownEvent):
            self._dead_letter_queue.add(event.model_dump(), REASON_UNKNOWN_EVENT_TYPE)
            return (1, 0)
        record = self._session_manager.process_event(event)
        if record is not None:
            self._writer.write(record)
            return (0, 1)
        return (0, 0)

    def shutdown(self) -> None:
        for record in self._session_manager.flush():
            self._writer.write(record)
        self._reader.mark_complete()
        self._reader.close()
