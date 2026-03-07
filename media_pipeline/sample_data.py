FROZEN_TIMESTAMP = "2026-03-07T12:00:00+00:00"

BASE = {
    "event_id": "123",
    "user_id": "user123",
    "content_id": "content123",
    "timestamp": FROZEN_TIMESTAMP,
    "schema_version": "1.0",
}

SAMPLE_RECORDS = [
    # 1 — play → stop, first user
    {**BASE, "event_type": "PLAY", "position_seconds": 10.0},
    {
        **BASE,
        "event_type": "STOP",
        "position_seconds": 10.0,
        "watch_duration_seconds": 30.0,
    },
    # 2 — play → stop, second user
    {
        **BASE,
        "event_id": "201",
        "user_id": "user456",
        "event_type": "PLAY",
        "position_seconds": 0.0,
    },
    {
        **BASE,
        "event_id": "202",
        "user_id": "user456",
        "event_type": "STOP",
        "position_seconds": 45.0,
        "watch_duration_seconds": 45.0,
        "completion_rate": 1.0,
    },
    # 3 — unknown optional field (schema evolution); unique user/content, no stop → flushed incomplete
    {
        **BASE,
        "event_id": "301",
        "user_id": "user_schema_test",
        "content_id": "content_schema_test",
        "event_type": "PLAY",
        "position_seconds": 0.0,
        "subtitle_language": "en",
    },
    # 4 — unknown event type
    {
        **BASE,
        "event_id": "401",
        "event_type": "SEEK",
        "position_seconds": 120.0,
    },
    # 5 — missing required field
    {
        "event_id": "501",
        "event_type": "PLAY",
        "content_id": "content123",
        "timestamp": FROZEN_TIMESTAMP,
    },
    # 6 — interleaved events, two users, same content
    {
        **BASE,
        "event_id": "601",
        "user_id": "userA",
        "content_id": "contentX",
        "event_type": "PLAY",
        "position_seconds": 0.0,
    },
    {
        **BASE,
        "event_id": "602",
        "user_id": "userB",
        "content_id": "contentX",
        "event_type": "PLAY",
        "position_seconds": 0.0,
    },
    {
        **BASE,
        "event_id": "603",
        "user_id": "userA",
        "content_id": "contentX",
        "event_type": "STOP",
        "position_seconds": 60.0,
        "watch_duration_seconds": 60.0,
    },
    {
        **BASE,
        "event_id": "604",
        "user_id": "userB",
        "content_id": "contentX",
        "event_type": "STOP",
        "position_seconds": 55.0,
        "watch_duration_seconds": 55.0,
    },
    # 7 — incomplete session, no stop
    {
        **BASE,
        "event_id": "701",
        "user_id": "user789",
        "event_type": "PLAY",
        "position_seconds": 0.0,
    },
    {
        **BASE,
        "event_id": "702",
        "user_id": "user789",
        "event_type": "PAUSE",
        "position_seconds": 15.0,
    },
]
