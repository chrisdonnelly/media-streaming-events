from dataclasses import dataclass

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
        raise NotImplementedError

    def shutdown(self) -> None:
        raise NotImplementedError
