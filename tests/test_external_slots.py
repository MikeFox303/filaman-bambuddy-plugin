import importlib.util
import json
from pathlib import Path

import pytest


_MODULE_PATH = Path(__file__).resolve().parents[1] / "bambuddy" / "external_slots.py"
_SPEC = importlib.util.spec_from_file_location("bambuddy_external_slots", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

canonical_external_tray_id = _MODULE.canonical_external_tray_id
external_slot_index = _MODULE.external_slot_index


def test_physical_external_trays_map_to_bambuddy_assignment_slots():
    assert canonical_external_tray_id(254) == 0
    assert canonical_external_tray_id(255) == 1
    assert external_slot_index(254) == "255-0"
    assert external_slot_index(255) == "255-1"


def test_already_canonical_external_slots_are_idempotent():
    assert canonical_external_tray_id(0) == 0
    assert canonical_external_tray_id(1) == 1


def test_unknown_external_tray_is_rejected():
    with pytest.raises(ValueError):
        canonical_external_tray_id(253)


def test_pending_snapshot_uses_canonical_external_slot_ids():
    source = Path(__file__).resolve().parents[1].joinpath("bambuddy", "driver.py").read_text(encoding="utf-8")
    start = source.index("async def _capture_pending_snapshot")
    tail = source[start:]
    end = tail.find("\n    async def ", 1)
    method = tail if end < 0 else tail[:end]
    assert "snap[external_slot_index(vt_id)]" in method
    assert 'snap[f"255-{vt_id}"]' not in method


def test_sync_actions_expose_ru_uk_labels():
    source = Path(__file__).resolve().parents[1].joinpath("bambuddy", "driver.py").read_text(encoding="utf-8")
    assert '"labelByLocale": {"ru": "Синхронизировать", "uk": "Синхронізувати"}' in source
    assert '"labelByLocale": {"ru": "Полная пересинхронизация", "uk": "Повна повторна синхронізація"}' in source


def test_manifest_version_is_1_3_9():
    manifest = json.loads(Path(__file__).resolve().parents[1].joinpath("bambuddy", "plugin.json").read_text(encoding="utf-8"))
    assert manifest["version"] == "1.3.9"
