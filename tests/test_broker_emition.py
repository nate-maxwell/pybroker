from typing import Any

import broker


def test_wildcard_parent_receives_child_events() -> None:
    """
    Test that a parent wildcard subscriber receives events from child
    namespaces.
    """
    broker.clear()
    parent_invoked: list[bool] = []
    child_invoked: list[bool] = []

    # noinspection PyUnusedLocal
    def parent_callback(**kwargs: Any) -> None:
        parent_invoked.append(True)

    # noinspection PyUnusedLocal
    def child_callback(**kwargs: Any) -> None:
        child_invoked.append(True)

    broker.register_subscriber("test.*", parent_callback)
    broker.register_subscriber("test.child", child_callback)
    broker.emit("test.child")

    # Assert - both parent wildcard and exact child should receive the event
    assert len(parent_invoked) == 1  # Parent gets it via wildcard match
    assert len(child_invoked) == 1  # Child gets it via exact match


def test_arguments_passed_to_callback() -> None:
    """Test that arguments are correctly passed from emit to the callback."""
    broker.clear()
    namespace = "system.io.file_save"
    received_kwargs: dict[str, object] = {}

    def test_callback(filename: str, size: int, success: bool) -> None:
        received_kwargs["filename"] = filename
        received_kwargs["size"] = size
        received_kwargs["success"] = success

    broker.register_subscriber(namespace, test_callback)
    broker.emit(namespace, filename="test.txt", size=1024, success=True)

    # Assert - all arguments should be passed correctly
    assert received_kwargs["filename"] == "test.txt"
    assert received_kwargs["size"] == 1024
    assert received_kwargs["success"] is True


def test_callbacks_execute_in_priority_order() -> None:
    """Test that callbacks execute in priority order (highest first)."""
    broker.clear()
    namespace = "system.test"
    execution_order: list[str] = []

    # noinspection PyUnusedLocal
    def low_priority_callback(**kwargs: Any) -> None:
        execution_order.append("low")

    # noinspection PyUnusedLocal
    def medium_priority_callback(**kwargs: Any) -> None:
        execution_order.append("medium")

    # noinspection PyUnusedLocal
    def high_priority_callback(**kwargs: Any) -> None:
        execution_order.append("high")

    broker.register_subscriber(namespace, medium_priority_callback, priority=5)
    broker.register_subscriber(namespace, high_priority_callback, priority=10)
    broker.register_subscriber(namespace, low_priority_callback, priority=1)
    broker.emit(namespace)

    # Assert - should execute in priority order (high to low)
    assert execution_order == ["high", "medium", "low"]
