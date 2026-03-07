from dataclasses import dataclass

from media_pipeline.pipeline.constants import (
    REASON_UNKNOWN_EVENT_TYPE,
    REASON_VALIDATION_FAILURE,
)
from media_pipeline.pipeline.models import UnknownEvent, parse_event
from media_pipeline.pipeline.writer import EventWriter


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
        processed = len(batch)
        dead_lettered = 0
        features_emitted = 0
        for raw in batch:
            event = parse_event(raw)
            if event is None:
                self._dead_letter_queue.add(raw, REASON_VALIDATION_FAILURE)
                dead_lettered += 1
            elif isinstance(event, UnknownEvent):
                self._dead_letter_queue.add(
                    event.model_dump(), REASON_UNKNOWN_EVENT_TYPE
                )
                dead_lettered += 1
            else:
                record = self._session_manager.process_event(event)
                if record is not None:
                    self._writer.write(record)
                    features_emitted += 1
        self._reader.mark_complete()
        return ProcessingResult(
            processed=processed,
            dead_lettered=dead_lettered,
            features_emitted=features_emitted,
        )

    def shutdown(self) -> None:
        for record in self._session_manager.flush():
            self._writer.write(record)
        self._reader.mark_complete()
        self._reader.close()
