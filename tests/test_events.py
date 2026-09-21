import uuid

import pytest

from orderstream.storage import validate_event


def event(**changes):
    value = {
        "event_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "schema_version": 1,
        "version": 1,
        "status": "created",
        "amount_cents": 200,
    }
    value.update(changes)
    return value


def test_valid_snapshot():
    assert validate_event(event())["status"] == "created"
    assert validate_event(event(status="cancelled", version=2))["version"] == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"amount_cents": True},
        {"amount_cents": -1},
        {"version": 3},
        {"status": "shipped"},
        {"event_id": "bad"},
        {"schema_version": 2},
    ],
)
def test_invalid_event(changes):
    with pytest.raises(ValueError):
        validate_event(event(**changes))
