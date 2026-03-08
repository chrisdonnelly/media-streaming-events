# Media Pipeline — Batch Processing Component

This prototype implements the batch processing stage of a media data pipeline: it reads raw streaming events, aggregates them into user-content sessions, and produces ML-ready feature records. It was chosen as the implementation component because it directly addresses the ML-readiness requirement, and provides a clear surface for demonstrating schema evolution handling.

## Where it fits

In the full pipeline, raw events flow from devices through a message broker (for example Kafka) to object storage via a connector service. This component picks up from there — reading those accumulated events, grouping them into sessions, and writing structured feature records that a data lakehouse platform could materialise into a feature store for model training and live recommendations.

The MockEventReader simulates reading a batch of raw events from object storage.

## ML-readiness

FeatureRecord is a flat, typed structure. Pre-aggregated metrics — total_watch_seconds, pause_count, completion_rate — are computed at pipeline time so downstream model training consumes ready-made features rather than raw events. session_start and session_end support time based feature extraction. Output is in JSONL format, natively readable by data lakehouse platforms. A schema_version field supports feature evolution without invalidating historical records.

## Schema evolution

Events are parsed using Pydantic models configured with extra='ignore' — unknown fields are silently dropped rather than causing failures. This means new fields can be added to events by producers without breaking the pipeline. Entirely unrecognised event types are routed to the dead letter queue rather than crashing — dead_letter.jsonl contains an example.

## How to run

This project uses [uv](https://github.com/astral-sh/uv) for dependency management.

From the project root:

```bash
uv sync
uv run python main.py      
```

Run tests:

```bash
uv run pytest
```

Type checking:
```bash
uv run pyrefly check
```

Linting:
```bash
uv run ruff format
uv run ruff check --fix
```

Output is written to `media_pipeline/output/`: feature records in `feature_records.jsonl`, dead letter entries in `dead_letter.jsonl`.

Sample input data is defined in `media_pipeline/sample_data.py` and covers the following scenarios:

- Complete sessions with and without pauses.
- Interleaved sessions across users and same content.
- Schema evolution (optional field accepted, no failure).
- Unknown event types (routed to dead letter).
- Validation failures (missing required field).
- Incomplete sessions flushed on shutdown.
