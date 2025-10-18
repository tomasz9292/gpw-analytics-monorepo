from datetime import datetime, timedelta

from api.ohlc_progress import OhlcSyncProgressTracker
from api.stooq_ohlc import OhlcSyncResult


def _make_result(**overrides) -> OhlcSyncResult:
    now = datetime.utcnow()
    payload = {
        "symbols": 3,
        "inserted": 120,
        "skipped": 1,
        "errors": ["CDR: timeout"],
        "started_at": now - timedelta(seconds=5),
        "finished_at": now,
        "truncated": False,
        "request_log": [],
        "requested_as_admin": True,
    }
    payload.update(overrides)
    return OhlcSyncResult(**payload)


def test_tracker_start_update_finish():
    tracker = OhlcSyncProgressTracker()

    tracker.start(total_symbols=5, requested_as_admin=True)
    tracker.update(
        processed_symbols=2,
        inserted_rows=40,
        skipped_symbols=1,
        current_symbol="CDR",
        errors=["CDR: timeout"],
    )

    running = tracker.snapshot()
    assert running.status == "running"
    assert running.total_symbols == 5
    assert running.processed_symbols == 2
    assert running.inserted_rows == 40
    assert running.skipped_symbols == 1
    assert running.current_symbol == "CDR"
    assert running.errors == ["CDR: timeout"]
    assert running.requested_as_admin is True

    result = _make_result(symbols=5, inserted=42, skipped=2)
    tracker.finish(result)

    finished = tracker.snapshot()
    assert finished.status == "success"
    assert finished.processed_symbols == 5
    assert finished.inserted_rows == 42
    assert finished.skipped_symbols == 2
    assert finished.current_symbol is None
    assert finished.errors == ["CDR: timeout"]
    assert finished.requested_as_admin is True
    assert finished.message == "Synchronizacja zakończona pomyślnie."


def test_tracker_fail_overrides_message_and_errors():
    tracker = OhlcSyncProgressTracker()
    tracker.start(total_symbols=2, requested_as_admin=False)
    tracker.update(
        processed_symbols=1,
        inserted_rows=10,
        skipped_symbols=0,
        current_symbol="PKN",
        errors=[],
    )

    tracker.fail("Błąd połączenia", errors=["PKN: HTTP 500"])

    failed = tracker.snapshot()
    assert failed.status == "error"
    assert failed.message == "Błąd połączenia"
    assert failed.errors == ["PKN: HTTP 500"]
    assert failed.finished_at is not None
    assert failed.current_symbol is None
    assert failed.started_at is not None


def test_tracker_update_is_monotonic():
    tracker = OhlcSyncProgressTracker()
    tracker.start(total_symbols=3, requested_as_admin=False)

    tracker.update(
        processed_symbols=2,
        inserted_rows=120,
        skipped_symbols=1,
        current_symbol="CDR",
        errors=["ostrzeżenie"],
    )

    # Kolejny snapshot z mniejszymi wartościami nie powinien cofnąć progresu.
    tracker.update(
        processed_symbols=1,
        inserted_rows=0,
        skipped_symbols=0,
        current_symbol="PKN",
        errors=["ostrzeżenie"],
    )

    snapshot = tracker.snapshot()
    assert snapshot.processed_symbols == 2
    assert snapshot.inserted_rows == 120
    assert snapshot.skipped_symbols == 1
    assert snapshot.current_symbol == "PKN"
