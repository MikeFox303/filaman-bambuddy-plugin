import asyncio

import pytest

from app.plugins.bambuddy.driver import Driver


def _driver():
    driver = object.__new__(Driver)
    driver._usage_reconcile_lock = asyncio.Lock()
    driver._usage_ledger_supported = None
    driver._usage_ledger_retry_at = 0.0
    driver._usage_legacy_allowed = False
    driver._bambuddy_printer_id = 3
    driver._spoolman_enabled = False
    return driver


@pytest.mark.asyncio
async def test_first_durable_contact_bootstraps_without_replaying_history():
    driver = _driver()
    stored = []
    writes = []

    async def load_cursor():
        return None

    async def fetch_page(after_id):
        assert after_id == 0
        return "supported", {
            "latest_id": 88,
            "events": [
                {
                    "usage_id": 70,
                    "filaman_spool_id": 42,
                    "weight_used": 12.0,
                    "created_at": "2026-08-20T10:00:00",
                }
            ],
        }

    async def store_cursor(value):
        stored.append(value)

    async def write(*args, **kwargs):
        writes.append((args, kwargs))
        return True

    driver._load_usage_cursor = load_cursor
    driver._fetch_usage_page = fetch_page
    driver._store_usage_cursor = store_cursor
    driver._write_durable_consumption = write

    assert await driver._reconcile_usage_events() == "supported"
    assert stored == [88]
    assert writes == []


@pytest.mark.asyncio
async def test_replay_acks_each_event_only_after_successful_write():
    driver = _driver()
    cursor = {"value": 10}
    stored = []
    writes = []

    async def load_cursor():
        return cursor["value"]

    async def fetch_page(after_id):
        assert after_id == 10
        return "supported", {
            "latest_id": 12,
            "events": [
                {
                    "usage_id": 11,
                    "filaman_spool_id": 41,
                    "spool_id": 101,
                    "weight_used": 4.25,
                    "created_at": "2026-08-22T12:00:00+00:00",
                },
                {
                    "usage_id": 12,
                    "filaman_spool_id": 42,
                    "spool_id": 102,
                    "weight_used": 7.5,
                    "created_at": "2026-08-22T12:05:00+00:00",
                },
            ],
        }

    async def store_cursor(value):
        stored.append(value)
        cursor["value"] = value

    async def write(spool_id, grams, *, source_event_key, event_at):
        writes.append((spool_id, grams, source_event_key, event_at.isoformat()))
        return True

    driver._load_usage_cursor = load_cursor
    driver._fetch_usage_page = fetch_page
    driver._store_usage_cursor = store_cursor
    driver._write_durable_consumption = write

    assert await driver._reconcile_usage_events() == "supported"
    assert stored == [11, 12]
    assert writes == [
        (41, 4.25, "bambuddy:usage:3:11:20260822T120000.000000Z", "2026-08-22T12:00:00+00:00"),
        (42, 7.5, "bambuddy:usage:3:12:20260822T120500.000000Z", "2026-08-22T12:05:00+00:00"),
    ]


@pytest.mark.asyncio
async def test_failed_second_write_does_not_ack_past_first_event():
    driver = _driver()
    stored = []

    async def load_cursor():
        return 20

    async def fetch_page(_after_id):
        return "supported", {
            "latest_id": 22,
            "events": [
                {
                    "usage_id": 21,
                    "filaman_spool_id": 41,
                    "weight_used": 1.0,
                    "created_at": "2026-08-22T12:00:00+00:00",
                },
                {
                    "usage_id": 22,
                    "filaman_spool_id": 42,
                    "weight_used": 2.0,
                    "created_at": "2026-08-22T12:01:00+00:00",
                },
            ],
        }

    async def store_cursor(value):
        stored.append(value)

    async def write(spool_id, *_args, **_kwargs):
        return spool_id == 41

    driver._load_usage_cursor = load_cursor
    driver._fetch_usage_page = fetch_page
    driver._store_usage_cursor = store_cursor
    driver._write_durable_consumption = write

    assert await driver._reconcile_usage_events() == "transient"
    assert stored == [21]


@pytest.mark.asyncio
async def test_unmapped_event_blocks_cursor_instead_of_skipping_usage():
    driver = _driver()
    stored = []
    writes = []

    async def load_cursor():
        return 30

    async def fetch_page(_after_id):
        return "supported", {
            "latest_id": 31,
            "events": [
                {
                    "usage_id": 31,
                    "spool_id": 777,
                    "filaman_spool_id": None,
                    "weight_used": 3.0,
                    "created_at": "2026-08-22T12:00:00+00:00",
                }
            ],
        }

    async def resolve(_spool_id):
        return None

    async def store_cursor(value):
        stored.append(value)

    async def write(*args, **kwargs):
        writes.append((args, kwargs))
        return True

    driver._load_usage_cursor = load_cursor
    driver._fetch_usage_page = fetch_page
    driver._resolve_bambuddy_spool_id = resolve
    driver._store_usage_cursor = store_cursor
    driver._write_durable_consumption = write

    assert await driver._reconcile_usage_events() == "transient"
    assert stored == []
    assert writes == []


@pytest.mark.asyncio
async def test_transient_ledger_failure_never_falls_back_to_ws_debit(monkeypatch):
    driver = _driver()
    legacy_calls = []

    async def reconcile():
        return "transient"

    async def legacy(_self, event):
        legacy_calls.append(event)

    driver._reconcile_usage_events = reconcile
    monkeypatch.setattr(Driver.__mro__[2], "_handle_spool_usage_logged", legacy)

    event = {"printer_id": 3, "usage": [{"spool_id": 1, "weight_used": 5.0}]}
    await driver._handle_spool_usage_logged(event)

    assert legacy_calls == []


@pytest.mark.asyncio
async def test_explicit_old_bambuddy_can_use_legacy_before_cursor_exists(monkeypatch):
    driver = _driver()
    driver._usage_legacy_allowed = True
    legacy_calls = []

    async def reconcile():
        return "unsupported"

    async def legacy(_self, event):
        legacy_calls.append(event)

    driver._reconcile_usage_events = reconcile
    monkeypatch.setattr(Driver.__mro__[2], "_handle_spool_usage_logged", legacy)

    event = {"printer_id": 3, "event_id": "old", "usage": []}
    await driver._handle_spool_usage_logged(event)

    assert legacy_calls == [event]
