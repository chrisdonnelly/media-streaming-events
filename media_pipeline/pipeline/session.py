from abc import ABC, abstractmethod

from pydantic import AwareDatetime

from media_pipeline.pipeline.constants import (
    REASON_DUPLICATE_STOP_EVENT,
    REASON_LATE_EVENT,
)
from media_pipeline.pipeline.models import (
    BaseEvent,
    FeatureRecord,
    PauseEvent,
    PlayEvent,
    StopEvent,
)


class DeadLetterQueue(ABC):
    @abstractmethod
    def add(self, raw: dict, reason: str) -> None: ...

    @abstractmethod
    def get_all(self) -> list[dict]: ...

    @abstractmethod
    def count(self) -> int: ...


class Session:
    def __init__(self, user_id: str, content_id: str, session_start: AwareDatetime):
        self.user_id = user_id
        self.content_id = content_id
        self.session_start = session_start
        self._session_end: AwareDatetime | None = None
        self._total_watch_seconds: float = 0.0
        self._pause_count: int = 0
        self._completion_rate: float | None = None
        self._event_count: int = 0
        self._has_stop: bool = False

    def add_event(self, event: BaseEvent) -> bool:
        if isinstance(event, StopEvent):
            return self._handle_stop_event(event)
        self._session_end = event.timestamp
        self._event_count += 1
        if isinstance(event, PauseEvent):
            self._pause_count += 1
        return True

    def _handle_stop_event(self, event: StopEvent) -> bool:
        if self._has_stop:
            return False
        self._session_end = event.timestamp
        self._event_count += 1
        self._total_watch_seconds = event.watch_duration_seconds
        self._completion_rate = event.completion_rate
        self._has_stop = True
        return True

    def is_complete(self) -> bool:
        return self._has_stop

    def to_feature_record(self) -> FeatureRecord:
        session_end = (
            self._session_end if self._session_end is not None else self.session_start
        )
        return FeatureRecord(
            user_id=self.user_id,
            content_id=self.content_id,
            session_start=self.session_start,
            session_end=session_end,
            total_watch_seconds=self._total_watch_seconds,
            pause_count=self._pause_count,
            completion_rate=self._completion_rate,
            event_count=self._event_count,
        )


class SessionManager:
    def __init__(self, dead_letter_queue: DeadLetterQueue):
        self._sessions: dict[tuple[str, str], Session] = {}
        self._closed_keys: set[tuple[str, str]] = set()
        self._dead_letter_queue = dead_letter_queue

    def process_event(
        self, event: PlayEvent | PauseEvent | StopEvent
    ) -> FeatureRecord | None:
        key = (event.user_id, event.content_id)
        if key in self._closed_keys:
            self._dead_letter_queue.add(event.model_dump(), REASON_LATE_EVENT)
            return None
        if key in self._sessions:
            session = self._sessions[key]
            if session.is_complete():
                self._dead_letter_queue.add(event.model_dump(), REASON_LATE_EVENT)
                return None
            if not session.add_event(event):
                self._dead_letter_queue.add(
                    event.model_dump(), REASON_DUPLICATE_STOP_EVENT
                )
                return None
            if session.is_complete():
                record = session.to_feature_record()
                del self._sessions[key]
                self._closed_keys.add(key)
                return record
            return None
        session = Session(event.user_id, event.content_id, event.timestamp)
        session.add_event(event)
        if session.is_complete():
            self._closed_keys.add(key)
            return session.to_feature_record()
        self._sessions[key] = session
        return None

    def flush(self) -> list[FeatureRecord]:
        records = [session.to_feature_record() for session in self._sessions.values()]
        self._sessions.clear()
        return records
