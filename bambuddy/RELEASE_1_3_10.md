# 1.3.10

Hotfix for FilaMan async consumption writes discovered during physical X2D Cloud acceptance.

- eager-load FilaMan spool relationships through `SpoolService.get_spool()` before consumption writes;
- retry transient consumption writes with the same idempotency key;
- regression coverage for the WebSocket/background-task write path.
