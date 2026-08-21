import pytest

from bambuddy.external_slots import canonical_external_tray_id, external_slot_index


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
