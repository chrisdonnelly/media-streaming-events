from datetime import datetime, timezone
from pathlib import Path

from media_pipeline.pipeline.consumer import MockEventReader
from media_pipeline.pipeline.processor import EventProcessor
from media_pipeline.pipeline.session import DeadLetterQueue, SessionManager
from media_pipeline.pipeline.writer import JSONWriter
from media_pipeline.sample_data import SAMPLE_RECORDS


class InMemoryDLQ(DeadLetterQueue):
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


def main():
    output_dir = Path(__file__).parent / "media_pipeline" / "output"
    reader = MockEventReader(records=SAMPLE_RECORDS)
    dlq = InMemoryDLQ()
    session_manager = SessionManager(dead_letter_queue=dlq)
    writer = JSONWriter(output_dir=output_dir)
    processor = EventProcessor(
        reader=reader,
        session_manager=session_manager,
        writer=writer,
        dead_letter_queue=dlq,
    )

    while not reader.is_empty():
        processor.process_batch()

    processor.shutdown()
    writer.write_dead_letter(dlq)

    print(f"Feature records written: {output_dir / 'feature_records.jsonl'}")
    print(f"Dead letter entries: {dlq.count()}")


if __name__ == "__main__":
    main()
