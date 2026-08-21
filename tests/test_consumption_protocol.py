import pytest

from app.plugins.bambuddy.driver import Driver


@pytest.mark.asyncio
async def test_spool_usage_logged_maps_bambuddy_id_and_builds_idempotency_key():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver.printer_id = 7
    driver._spoolman_enabled = False
    driver._spoolman_enabled = False
    driver._slot_to_filaman_spool = {}
    calls = []
    driver._resolve_bambuddy_spool_id = lambda spool_id: _resolved(42, spool_id)
    driver._report_consumption = lambda spool, grams, **kw: _record(calls, spool, grams, kw)

    await driver._handle_spool_usage_logged({
        "type": "spool_usage_logged",
        "printer_id": 3,
        "event_id": "bambuddy:3:job-1",
        "usage": [{"spool_id": 17, "weight_used": 12.4, "ams_id": 0, "tray_id": 2}],
    })

    assert calls == [(42, 12.4, {"source_event_key": "bambuddy:3:job-1:17:0:2"})]


@pytest.mark.asyncio
async def test_spool_usage_logged_ignores_invalid_weight_and_unsafe_mapping():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver._spoolman_enabled = False
    driver._modern_usage_event_ids = set()
    driver._legacy_consumption_tasks = {}
    driver._slot_to_filaman_spool = {}
    driver._resolve_bambuddy_spool_id = lambda _: _resolved(None, None)
    calls = []
    driver._report_consumption = lambda *args, **kw: _record(calls, *args, **kw)

    await driver._handle_spool_usage_logged({
        "printer_id": 3,
        "event_id": "event",
        "usage": [
            {"spool_id": 1, "weight_used": 0, "ams_id": 0, "tray_id": 0},
            {"spool_id": 2, "weight_used": "NaN", "ams_id": 0, "tray_id": 1},
            {"spool_id": 3, "weight_used": 5, "ams_id": 1, "tray_id": 1},
        ],
    })

    assert calls == []


async def _resolved(value, _expected):
    return value


async def _record(calls, spool, grams, kw):
    calls.append((spool, grams, kw))


@pytest.mark.asyncio
async def test_modern_print_complete_does_not_consume():
    driver = object.__new__(Driver)
    driver._spoolman_enabled = False
    driver._slot_to_filaman_spool = {"0-0": 42}
    driver._report_consumption = lambda *args, **kwargs: _record([], *args, **kwargs)

    await driver._handle_print_complete({"weight_used": 12.4}, event_id="run-1")


@pytest.mark.asyncio
async def test_modern_usage_without_bambuddy_spool_id_uses_slot_fallback():
    driver = object.__new__(Driver)
    driver._spoolman_enabled = False
    driver._modern_usage_event_ids = set()
    driver._legacy_consumption_tasks = {}
    driver._slot_to_filaman_spool = {"255-1": 99}
    driver._resolve_bambuddy_spool_id = lambda _: _resolved(None, None)
    calls = []
    driver._report_consumption = lambda spool, grams, **kw: _record(calls, spool, grams, kw)

    await driver._handle_spool_usage_logged({
        "printer_id": 3,
        "event_id": "run-2",
        "usage": [{"weight_used": 3.2, "ams_id": 255, "tray_id": 1}],
    })

    assert calls == [(99, 3.2, {"source_event_key": "run-2:None:255:1"})]


@pytest.mark.asyncio
async def test_modern_usage_without_event_id_does_not_create_permanent_legacy_key():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver.printer_id = 7
    driver._spoolman_enabled = False
    driver._slot_to_filaman_spool = {}
    calls = []
    driver._resolve_bambuddy_spool_id = lambda spool_id: _resolved(42, spool_id)
    driver._report_consumption = lambda spool, grams, **kw: _record(calls, spool, grams, kw)

    event = {
        "type": "spool_usage_logged",
        "printer_id": 3,
        "usage": [{"spool_id": 17, "weight_used": 1.5, "ams_id": 0, "tray_id": 2}],
    }
    await driver._handle_spool_usage_logged(event)
    await driver._handle_spool_usage_logged(event)

    assert calls == [
        (42, 1.5, {"source_event_key": None}),
        (42, 1.5, {"source_event_key": None}),
    ]


@pytest.mark.asyncio
async def test_legacy_print_complete_reprints_do_not_collide_on_filename_key():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver._spoolman_enabled = False
    driver._slot_to_filaman_spool = {"0-0": 42}
    calls = []
    driver._report_consumption = lambda spool, grams, **kw: _record(calls, spool, grams, kw)

    payload = {"weight_used": 2.0, "filename": "same-file.3mf"}
    await driver._handle_print_complete(payload, event_id="")
    await driver._handle_print_complete(payload, event_id="")

    assert calls == [
        (42, 2.0, {"source_event_key": None}),
        (42, 2.0, {"source_event_key": None}),
    ]


@pytest.mark.asyncio
async def test_report_consumption_db_uses_eager_service_loader(monkeypatch):
    """Regression: WebSocket consumption must not lazy-load Spool.status.

    The physical X2D test exposed SQLAlchemy MissingGreenlet because the driver
    used AsyncSession.get(Spool) and SpoolService.record_consumption later read
    spool.status. The driver must load through SpoolService.get_spool(), whose
    query eagerly loads status/filament relationships.
    """
    driver = object.__new__(Driver)
    sentinel_spool = object()
    calls = []

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return None
        async def get(self, *args, **kwargs):
            raise AssertionError("raw AsyncSession.get(Spool) must not be used")

    class FakeSessionFactory:
        def __call__(self):
            return FakeSession()

    class FakeService:
        def __init__(self, db):
            self.db = db
        async def get_spool(self, spool_id):
            calls.append(("get", spool_id))
            return sentinel_spool
        async def record_consumption(self, **kwargs):
            calls.append(("record", kwargs))
            return object(), 275.0

    import app.plugins.bambuddy.driver as driver_module
    monkeypatch.setattr(driver_module, "async_session_maker", FakeSessionFactory())
    monkeypatch.setattr(driver_module, "SpoolService", FakeService)

    await driver._report_consumption_db(
        5, 5.0, source_event_key="run-physical:5:0:1"
    )

    assert calls[0] == ("get", 5)
    assert calls[1][0] == "record"
    assert calls[1][1]["spool"] is sentinel_spool
    assert calls[1][1]["source_event_key"] == "run-physical:5:0:1"


@pytest.mark.asyncio
async def test_report_consumption_retries_with_same_idempotency_key(monkeypatch):
    driver = object.__new__(Driver)
    attempts = []

    async def flaky(spool_id, grams, *, source_event_key=None):
        attempts.append((spool_id, grams, source_event_key))
        if len(attempts) < 3:
            raise RuntimeError("temporary write failure")

    driver._report_consumption_db = flaky
    monkeypatch.setattr("app.plugins.bambuddy.driver.asyncio.sleep", _no_sleep)

    await driver._report_consumption(
        5, 5.0, source_event_key="stable-run:17:0:1"
    )

    assert attempts == [
        (5, 5.0, "stable-run:17:0:1"),
        (5, 5.0, "stable-run:17:0:1"),
        (5, 5.0, "stable-run:17:0:1"),
    ]


async def _no_sleep(_delay):
    return None
