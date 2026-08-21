# Consumption MissingGreenlet hotfix

Physical X2D acceptance on 2026-08-21 confirmed the Cloud/Bambuddy usage calculation succeeded but FilaMan did not debit the mapped spool.

FilaMan log:

`Failed to report consumption for FilaMan spool 5: greenlet_spawn has not been called; can't call await_only() here.`

Root cause: the Bambuddy plugin loaded `Spool` with raw `AsyncSession.get()` and then passed it to `SpoolService.record_consumption()`. That service can inspect `spool.status` for automatic status transition, causing an async lazy-load from a background WebSocket task and SQLAlchemy `MissingGreenlet`.

Required fix: load the spool through `SpoolService.get_spool()` (eager relationships) and retry transient writes with the same idempotency key.
