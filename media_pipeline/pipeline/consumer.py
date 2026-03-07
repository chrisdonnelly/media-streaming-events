from abc import ABC, abstractmethod

MAX_RECORDS_PER_READ = 100


class EventReader(ABC):
    @abstractmethod
    def read(self) -> list[dict]: ...

    @abstractmethod
    def mark_complete(self) -> None: ...

    @abstractmethod
    def is_empty(self) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...


class MockEventReader(EventReader):
    def __init__(self, records: list[dict]):
        self._records = list(records)
        self._closed = False
        # Test instrumentation only — not part of EventReader interface.
        self.batches_marked_complete = 0

    def read(self) -> list[dict]:
        if self._closed:
            return []
        batch = self._records[:MAX_RECORDS_PER_READ]
        del self._records[: len(batch)]
        return batch

    def mark_complete(self) -> None:
        self.batches_marked_complete += 1

    def close(self) -> None:
        self._closed = True

    def is_empty(self) -> bool:
        return len(self._records) == 0
