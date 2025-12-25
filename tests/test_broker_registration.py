from typing import Any

import pytest

import broker


def test_record_protected() -> None:
    """Test that the subscriber dict in the broker is protected by closure."""
    broker.clear()
    try:
        _ = "foo" in broker._SUBSCRIBERS
    except Exception as e:
        assert type(e) == AttributeError


def test_subscriber_registration() -> None:
    """Test that a subscriber is successfully registered to a namespace."""
    broker.clear()
    namespace = "test.test_subscriber_registration"
    callback_invoked: list = []

    # noinspection PyUnusedLocal
    def test_callback(**kwargs: Any) -> None:
        callback_invoked.append(True)

    broker.register_subscriber(namespace, test_callback)
    broker.emit(namespace)

    # Assert - if subscriber was registered, callback should have been invoked
    assert len(callback_invoked) == 1
    assert callback_invoked[0] is True


def test_subscriber_unregistration() -> None:
    """Test that a subscriber is successfully unregistered from a namespace."""
    broker.clear()
    namespace = "test.test_subscriber_unregistration"
    callback_invoked: list[bool] = []

    # noinspection PyUnusedLocal
    def test_callback(**kwargs: Any) -> None:
        callback_invoked.append(True)

    broker.register_subscriber(namespace, test_callback)
    broker.unregister_subscriber(namespace, test_callback)
    broker.emit(namespace)

    # Assert - if subscriber was unregistered, callback should NOT have been
    # invoked
    assert len(callback_invoked) == 0
