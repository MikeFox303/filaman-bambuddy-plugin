# 1.3.11

Durable Bambuddy → FilaMan filament-consumption delivery for X2D/AMS production use.

- adds persisted Bambuddy usage-ledger replay with a per-printer FilaMan cursor;
- advances the cursor only after `SpoolService.record_consumption()` commits;
- preserves the existing `source_event_key` idempotency and MissingGreenlet-safe eager loading;
- catches usage missed during WebSocket disconnects, service restarts, and FINISH-time outages;
- uses a persisted first-upgrade boundary so a print completing during the upgrade is not lost;
- keeps explicit legacy fallback for older Bambuddy versions without mixing legacy and durable debit keys;
- preserves `inventory_only`, Cloud/Handy operation, X2D external-slot normalization, profile synchronization, and localization.
