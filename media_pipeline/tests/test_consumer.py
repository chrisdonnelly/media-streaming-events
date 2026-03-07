from media_pipeline.pipeline.consumer import MAX_RECORDS_PER_READ, MockEventReader


def test_read_returns_records_in_order():
    records = [{"id": 1}, {"id": 2}, {"id": 3}]
    reader = MockEventReader(records)
    batch = reader.read()
    assert batch == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_read_removes_records_from_queue():
    records = [{"id": 1}, {"id": 2}, {"id": 3}]
    reader = MockEventReader(records)
    first_batch = reader.read()
    assert first_batch == [{"id": 1}, {"id": 2}, {"id": 3}]
    second_batch = reader.read()
    assert second_batch == []


def test_read_returns_empty_when_no_records():
    reader = MockEventReader([])
    assert reader.read() == []


def test_read_caps_at_100_records():
    records = [{"id": i} for i in range(150)]
    reader = MockEventReader(records)
    batch = reader.read()
    assert len(batch) == MAX_RECORDS_PER_READ
    assert batch[0] == {"id": 0}
    assert batch[-1] == {"id": 99}
    remaining = reader.read()
    assert len(remaining) == 50


def test_read_returns_empty_after_close():
    records = [{"id": 1}]
    reader = MockEventReader(records)
    reader.close()
    assert reader.read() == []


def test_read_after_close_does_not_return_more():
    records = [{"id": 1}, {"id": 2}]
    reader = MockEventReader(records)
    reader.close()
    batch = reader.read()
    assert batch == []
    assert reader.read() == []


def test_mark_complete_increments_batches_marked_complete():
    reader = MockEventReader([{"id": 1}, {"id": 2}])
    assert reader.batches_marked_complete == 0
    reader.read()
    reader.mark_complete()
    assert reader.batches_marked_complete == 1
    reader.read()
    reader.mark_complete()
    assert reader.batches_marked_complete == 2


def test_mark_complete_runs_without_error():
    reader = MockEventReader([{"id": 1}])
    reader.read()
    assert reader.mark_complete() is None


def test_is_empty():
    reader = MockEventReader([{"id": 1}])
    assert reader.is_empty() is False
    reader.read()
    assert reader.is_empty() is True


def test_is_empty_when_initialized_with_no_records():
    reader = MockEventReader([])
    assert reader.is_empty() is True


def test_close_runs_without_error():
    reader = MockEventReader([{"id": 1}])
    assert reader.close() is None
    assert reader.read() == []
