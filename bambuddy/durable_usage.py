"""Durable Bambuddy → FilaMan consumption delivery.

WebSocket ``spool_usage_logged`` remains the low-latency wake-up signal, but
Bambuddy's persisted ``spool_usage_history`` is the source of truth.  This
mixin reconciles that ledger with a per-printer cursor stored in FilaMan and
advances the cursor only after the local idempotent consumption write commits.

Delivery semantics are therefore at-least-once, while FilaMan's
``source_event_key`` makes the debit effectively exactly-once.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from app.core.database import async_session_maker
from app.models.printer import Printer
from app.services.spool_service import SpoolService

logger = logging.getLogger(__name__)

_CURSOR_FIELD = "bambuddy_usage_cursors"
_REPLAY_INTERVAL_SECONDS = 60.0
_CAPABILITY_RETRY_SECONDS = 300.0
_PAGE_SIZE = 200


class DurableUsageMixin:
    """Add restart/reconnect-safe Bambuddy usage reconciliation to a driver."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._usage_replay_task: asyncio.Task | None = None
        self._usage_reconcile_lock = asyncio.Lock()
        # None = not probed yet; True = durable endpoint available; False =
        # confirmed old/unsupported Bambuddy.  A transient HTTP failure never
        # flips this to False.
        self._usage_ledger_supported: bool | None = None
        self._usage_ledger_retry_at: float = 0.0
        # Legacy direct WebSocket debit is allowed only before this FilaMan
        # printer has ever bootstrapped a durable cursor. Once a cursor exists,
        # mixing legacy event_id keys with durable usage-id keys could double
        # charge the same physical print after a later replay.
        self._usage_legacy_allowed: bool = False

    async def start(self) -> None:
        await super().start()
        if getattr(self, "_spoolman_enabled", False):
            return
        self._usage_replay_task = asyncio.create_task(self._usage_reconcile_loop())
        # Base driver already has a guarded task callback. This task catches its
        # own reconciliation errors, so the callback is only a final safety net.
        on_done = getattr(self, "_on_task_done", None)
        if callable(on_done):
            self._usage_replay_task.add_done_callback(on_done)

    async def stop(self) -> None:
        # Cancel before base.stop() closes the shared httpx client.
        task = getattr(self, "_usage_replay_task", None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._usage_replay_task = None
        await super().stop()

    # ------------------------------------------------------------------
    # Persistent cursor
    # ------------------------------------------------------------------

    def _usage_cursor_key(self) -> str:
        return str(getattr(self, "_bambuddy_printer_id", "") or "")

    async def _load_usage_cursor(self) -> int | None:
        """Read this Bambuddy printer's last acknowledged durable usage ID."""
        printer_id = getattr(self, "printer_id", None)
        if not printer_id:
            return None
        async with async_session_maker() as db:
            printer = await db.get(Printer, int(printer_id))
            if printer is None:
                return None
            custom_fields = dict(printer.custom_fields or {})
            cursors = custom_fields.get(_CURSOR_FIELD)
            if not isinstance(cursors, dict):
                return None
            raw = cursors.get(self._usage_cursor_key())
            try:
                value = int(raw)
            except (TypeError, ValueError):
                return None
            return value if value >= 0 else None

    async def _store_usage_cursor(self, cursor: int) -> None:
        """Persist an ACK cursor only after a local debit has committed."""
        printer_id = getattr(self, "printer_id", None)
        if not printer_id:
            raise RuntimeError("FilaMan printer_id is unavailable for usage cursor")
        async with async_session_maker() as db:
            printer = await db.get(Printer, int(printer_id))
            if printer is None:
                raise RuntimeError(f"FilaMan printer {printer_id} not found for usage cursor")
            custom_fields = dict(printer.custom_fields or {})
            raw_cursors = custom_fields.get(_CURSOR_FIELD)
            cursors = dict(raw_cursors) if isinstance(raw_cursors, dict) else {}
            cursors[self._usage_cursor_key()] = int(cursor)
            custom_fields[_CURSOR_FIELD] = cursors
            # Assign a fresh object so SQLAlchemy's JSON change tracking does
            # not depend on MutableDict instrumentation.
            printer.custom_fields = custom_fields
            await db.commit()

    # ------------------------------------------------------------------
    # Bambuddy durable ledger
    # ------------------------------------------------------------------

    async def _fetch_usage_page(self, after_id: int) -> tuple[str, dict | None]:
        """Return (supported|unsupported|transient, JSON payload)."""
        client = getattr(self, "_client", None)
        bambuddy_url = getattr(self, "_bambuddy_url", "")
        bambuddy_printer_id = getattr(self, "_bambuddy_printer_id", None)
        if client is None or not bambuddy_url or not bambuddy_printer_id:
            return "transient", None

        try:
            response = await client.get(
                f"{bambuddy_url}/api/v1/inventory/usage-events",
                params={
                    "printer_id": int(bambuddy_printer_id),
                    "after_id": int(after_id),
                    "limit": _PAGE_SIZE,
                },
            )
        except (httpx.HTTPError, OSError) as exc:
            logger.warning("Durable usage ledger request failed: %s", exc)
            return "transient", None
        except Exception as exc:
            logger.warning("Durable usage ledger request failed: %s", exc)
            return "transient", None

        if response.status_code in (404, 405):
            return "unsupported", None
        try:
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            logger.warning(
                "Durable usage ledger returned unusable response (HTTP %s): %s",
                response.status_code,
                exc,
            )
            return "transient", None
        if not isinstance(payload, dict) or not isinstance(payload.get("events", []), list):
            logger.warning("Durable usage ledger returned malformed JSON")
            return "transient", None
        return "supported", payload

    async def _reconcile_usage_events(self) -> str:
        """Replay durable usage rows in order and ACK strictly after commit.

        Returns one of ``supported``, ``unsupported`` or ``transient``.  The
        latter two never advance a cursor.
        """
        # Unit tests that construct Driver via object.__new__ intentionally have
        # no mixin state. Keep their legacy protocol tests meaningful and make
        # this extension backward-compatible with unusual third-party creation.
        lock = getattr(self, "_usage_reconcile_lock", None)
        if lock is None:
            return "unsupported"

        async with lock:
            cursor = await self._load_usage_cursor()
            request_after = cursor if cursor is not None else 0

            while True:
                state, payload = await self._fetch_usage_page(request_after)
                if state != "supported" or payload is None:
                    if state == "unsupported":
                        self._usage_ledger_supported = False
                        self._usage_ledger_retry_at = time.monotonic() + _CAPABILITY_RETRY_SECONDS
                        # Safe legacy compatibility only before a durable cursor
                        # has ever existed on this FilaMan printer.
                        self._usage_legacy_allowed = cursor is None
                    return state

                self._usage_ledger_supported = True
                self._usage_ledger_retry_at = 0.0
                self._usage_legacy_allowed = False

                try:
                    latest_id = int(payload.get("latest_id") or 0)
                except (TypeError, ValueError):
                    logger.warning("Durable usage ledger latest_id is invalid")
                    return "transient"
                if latest_id < 0:
                    return "transient"

                if cursor is None:
                    # Upgrade bootstrap: existing rows may already have been
                    # delivered with legacy WebSocket keys. Start at the current
                    # end of the ledger instead of double-debiting history.
                    await self._store_usage_cursor(latest_id)
                    logger.info(
                        "Initialized durable Bambuddy usage cursor for printer %s at %d; "
                        "historical usage was not replayed",
                        getattr(self, "_bambuddy_printer_id", None),
                        latest_id,
                    )
                    return "supported"

                if latest_id < cursor:
                    # A Bambuddy DB/volume reset can reuse integer IDs. Never
                    # auto-rewind because that risks colliding with already
                    # processed durable keys. Preserve data safety and require an
                    # explicit recovery decision instead.
                    logger.error(
                        "Bambuddy durable usage ledger moved backwards for printer %s "
                        "(cursor=%d, latest=%d); refusing automatic cursor reset",
                        getattr(self, "_bambuddy_printer_id", None),
                        cursor,
                        latest_id,
                    )
                    return "transient"

                events = payload.get("events", [])
                if not events:
                    return "supported"

                # Server promises ascending order; sort defensively so ACK can
                # never leap over an earlier event if a future implementation
                # changes the query order.
                try:
                    ordered = sorted(events, key=lambda item: int(item.get("usage_id", -1)))
                except Exception:
                    logger.warning("Durable usage ledger contains invalid usage IDs")
                    return "transient"

                processed_this_page = 0
                for item in ordered:
                    if not isinstance(item, dict):
                        return "transient"
                    try:
                        usage_id = int(item.get("usage_id"))
                    except (TypeError, ValueError):
                        return "transient"
                    if usage_id <= cursor:
                        continue

                    weight = _finite_float(item.get("weight_used"))
                    if weight is None:
                        logger.warning("Usage event %s has invalid weight; leaving cursor unchanged", usage_id)
                        return "transient"

                    filaman_spool_id = _positive_int(item.get("filaman_spool_id"))
                    if filaman_spool_id is None:
                        bambuddy_spool_id = _positive_int(item.get("spool_id"))
                        if bambuddy_spool_id is not None:
                            filaman_spool_id = await self._resolve_bambuddy_spool_id(bambuddy_spool_id)
                    if filaman_spool_id is None:
                        logger.warning(
                            "Cannot map durable Bambuddy usage event %s to a FilaMan spool; "
                            "will retry without advancing cursor",
                            usage_id,
                        )
                        return "transient"

                    # A non-positive ledger row needs no debit but is still a
                    # durable event that must be acknowledged to avoid blocking
                    # all later valid rows forever.
                    if weight > 0:
                        event_at = _parse_event_at(item.get("created_at"))
                        source_key = (
                            f"bambuddy:usage:{getattr(self, '_bambuddy_printer_id', '')}:"
                            f"{usage_id}:{_event_stamp(event_at)}"
                        )
                        ok = await self._write_durable_consumption(
                            int(filaman_spool_id),
                            weight,
                            source_event_key=source_key,
                            event_at=event_at,
                        )
                        if not ok:
                            return "transient"

                    await self._store_usage_cursor(usage_id)
                    cursor = usage_id
                    request_after = usage_id
                    processed_this_page += 1

                if processed_this_page == 0:
                    return "supported"
                if len(events) < _PAGE_SIZE:
                    return "supported"

    async def _write_durable_consumption(
        self,
        filaman_spool_id: int,
        delta_g: float,
        *,
        source_event_key: str,
        event_at: datetime,
    ) -> bool:
        """Write one debit with retry; success is the only durable ACK condition."""
        last_error: Exception | None = None
        for attempt in range(1, 4):
            try:
                async with async_session_maker() as db:
                    service = SpoolService(db)
                    # Same eager-loading rule as the base driver's hardened path:
                    # never raw db.get(Spool), otherwise spool.status may lazy-load
                    # from this background task and raise MissingGreenlet.
                    spool = await service.get_spool(filaman_spool_id)
                    if not spool:
                        logger.warning(
                            "FilaMan spool %s not found for durable usage event %s",
                            filaman_spool_id,
                            source_event_key,
                        )
                        return False
                    await service.record_consumption(
                        spool=spool,
                        delta_weight_g=delta_g,
                        event_at=event_at,
                        principal=None,
                        source="bambuddy",
                        source_event_key=source_event_key,
                    )
                logger.info(
                    "Durably recorded %.2fg for FilaMan spool %s (%s)",
                    delta_g,
                    filaman_spool_id,
                    source_event_key,
                )
                return True
            except Exception as exc:
                last_error = exc
                logger.warning(
                    "Durable consumption write attempt %d/3 failed for FilaMan spool %s: %s",
                    attempt,
                    filaman_spool_id,
                    exc,
                )
                if attempt < 3:
                    await asyncio.sleep(0.25 * attempt)

        logger.error(
            "Durable consumption write failed after 3 attempts for FilaMan spool %s: %s",
            filaman_spool_id,
            last_error,
        )
        return False

    # ------------------------------------------------------------------
    # Live wake-up + periodic safety net
    # ------------------------------------------------------------------

    async def _handle_spool_usage_logged(self, event: dict) -> None:
        """Use live WS as a wake-up signal, not as the durable debit payload."""
        if getattr(self, "_spoolman_enabled", False):
            return

        # Preserve the old driver's behavior for tests/third-party construction
        # that bypass __init__, where durable reconciliation is not initialized.
        if getattr(self, "_usage_reconcile_lock", None) is None:
            await super()._handle_spool_usage_logged(event)
            return

        now = time.monotonic()
        if (
            self._usage_ledger_supported is False
            and now < self._usage_ledger_retry_at
        ):
            if self._usage_legacy_allowed:
                await super()._handle_spool_usage_logged(event)
            return

        state = await self._reconcile_usage_events()
        if state == "unsupported" and self._usage_legacy_allowed:
            await super()._handle_spool_usage_logged(event)
        # On a transient error we intentionally do NOT debit from the WS body.
        # The persisted Bambuddy row will be retried later with the durable key.

    async def _usage_reconcile_loop(self) -> None:
        """Catch anything missed by WebSocket reconnects or FINISH-time outages."""
        while getattr(self, "_running", False):
            try:
                if not getattr(self, "_spoolman_enabled", False):
                    await self._reconcile_usage_events()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Durable usage reconciliation failed: %s", exc, exc_info=True)

            try:
                await asyncio.sleep(_REPLAY_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                raise


def _positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_event_at(value: Any) -> datetime:
    if value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (TypeError, ValueError):
            pass
    return datetime.now(timezone.utc)


def _event_stamp(event_at: datetime) -> str:
    """Stable compact timestamp also protects keys if a Bambuddy DB is rebuilt."""
    return event_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
