import json
from abc import ABC, abstractmethod
from pathlib import Path

from media_pipeline.pipeline.models import FeatureRecord
from media_pipeline.pipeline.session import DeadLetterQueue


class EventWriter(ABC):
    @abstractmethod
    def write(self, record: FeatureRecord) -> None: ...


class JSONWriter(EventWriter):
    def __init__(self, output_dir: str | Path):
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._feature_path = self._output_dir / "feature_records.jsonl"
        self._dead_letter_path = self._output_dir / "dead_letter.jsonl"

    def write(self, record: FeatureRecord) -> None:
        with open(self._feature_path, "a") as f:
            f.write(record.model_dump_json() + "\n")

    def _json_default(self, o):
        if hasattr(o, "isoformat"):
            return o.isoformat()
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    def write_dead_letter(self, dlq: DeadLetterQueue) -> None:
        entries = dlq.get_all()
        with open(self._dead_letter_path, "a") as f:
            for entry in entries:
                out = {
                    "raw": entry["raw"],
                    "reason": entry["reason"],
                    "timestamp": (
                        entry["timestamp"].isoformat()
                        if hasattr(entry["timestamp"], "isoformat")
                        else entry["timestamp"]
                    ),
                }
                f.write(json.dumps(out, default=self._json_default) + "\n")
