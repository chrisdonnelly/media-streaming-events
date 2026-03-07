import json
from datetime import datetime, timezone

from media_pipeline.pipeline.models import FeatureRecord
from media_pipeline.pipeline.writer import JSONWriter
from media_pipeline.tests.conftest import ListDLQ


def test_write_creates_feature_records_file(tmp_path):
    writer = JSONWriter(tmp_path)
    record = FeatureRecord(
        user_id="u1",
        content_id="c1",
        session_start=datetime.now(timezone.utc),
        session_end=datetime.now(timezone.utc),
        total_watch_seconds=10.0,
        pause_count=0,
        completion_rate=0.5,
        event_count=2,
    )
    writer.write(record)
    path = tmp_path / "feature_records.jsonl"
    assert path.exists()
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["user_id"] == "u1"
    assert data["content_id"] == "c1"
    assert data["event_count"] == 2
    assert isinstance(data["session_start"], str)
    assert isinstance(data["session_end"], str)


def test_write_appends_records(tmp_path):
    writer = JSONWriter(tmp_path)
    base = datetime.now(timezone.utc)
    for i in range(2):
        record = FeatureRecord(
            user_id="u1",
            content_id="c1",
            session_start=base,
            session_end=base,
            total_watch_seconds=float(i),
            pause_count=0,
            completion_rate=None,
            event_count=1,
        )
        writer.write(record)
    lines = (tmp_path / "feature_records.jsonl").read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["total_watch_seconds"] == 0.0
    assert json.loads(lines[1])["total_watch_seconds"] == 1.0


def test_write_serialises_datetimes_iso(tmp_path):
    writer = JSONWriter(tmp_path)
    t = datetime(2026, 3, 7, 12, 0, 0, tzinfo=timezone.utc)
    record = FeatureRecord(
        user_id="u1",
        content_id="c1",
        session_start=t,
        session_end=t,
        total_watch_seconds=0.0,
        pause_count=0,
        completion_rate=None,
        event_count=1,
    )
    writer.write(record)
    data = json.loads((tmp_path / "feature_records.jsonl").read_text().strip())
    assert "T" in data["session_start"] and "2026" in data["session_start"]
    assert "T" in data["session_end"] and "2026" in data["session_end"]


def test_write_dead_letter_creates_file(tmp_path):
    dlq = ListDLQ()
    dlq.add({"event_id": "e1", "event_type": "SEEK"}, "unknown_event_type")
    writer = JSONWriter(tmp_path)
    writer.write_dead_letter(dlq)
    path = tmp_path / "dead_letter.jsonl"
    assert path.exists()
    lines = path.read_text().strip().split("\n")
    assert len(lines) == 1
    data = json.loads(lines[0])
    assert data["raw"]["event_id"] == "e1"
    assert data["reason"] == "unknown_event_type"
    assert isinstance(data["timestamp"], str)
    assert "T" in data["timestamp"]


def test_write_dead_letter_appends_entries(tmp_path):
    dlq = ListDLQ()
    dlq.add({"a": 1}, "reason1")
    dlq.add({"b": 2}, "reason2")
    writer = JSONWriter(tmp_path)
    writer.write_dead_letter(dlq)
    lines = (tmp_path / "dead_letter.jsonl").read_text().strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0])["reason"] == "reason1"
    assert json.loads(lines[1])["reason"] == "reason2"


def test_init_creates_output_dir(tmp_path):
    output_dir = tmp_path / "nested" / "dir"
    assert not output_dir.exists()
    JSONWriter(output_dir)
    assert output_dir.exists()
    assert output_dir.is_dir()
