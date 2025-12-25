import sys
from typing import Any
from typing import Callable
from types import ModuleType


CALLBACK = Callable[[Any], None]
"""
The callback end point that event info is forwarded to. These are the actions
that 'subscribe' and will execute when an event is triggered. Can be sync or
async.

Callbacks should return None as the broker cannot determine which value to
send back to the caller. If you want data back, create an event going the
opposite direction.
"""

_SUBSCRIBERS: dict[str, list[CALLBACK]] = {}
"""The broker's record of each namespace to callback.

This is kept outside of the replaced module class to create a protected
closure around the event topic:subscriber structure.
"""


class Broker(ModuleType):
    """Message broker system with hierarchical namespaces."""

    def __init__(self, name: str) -> None:
        super().__init__(name)

    @staticmethod
    def clear() -> None:
        _SUBSCRIBERS.clear()

    @staticmethod
    def register_subscriber(namespace: str, callback: CALLBACK) -> None:
        """
        Register a callback function to a namespace.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open' or
                'system.*').
            callback (Callable): Function to call when events are emitted.
        """
        if namespace not in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = []
        _SUBSCRIBERS[namespace].append(callback)

    @staticmethod
    def unregister_subscriber(namespace: str, callback: CALLBACK) -> None:
        """
        Remove a callback from a namespace.

        Args:
            namespace (str): Event namespace.
            callback (Callable): Function to remove.
        """
        if namespace in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace].remove(callback)
            if not _SUBSCRIBERS[namespace]:
                del _SUBSCRIBERS[namespace]

    def emit(self, namespace: str, **kwargs: Any) -> None:
        """
        Emit an event to all matching subscribers.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open').
            **kwargs: Arguments to pass to subscriber callbacks.
        """
        for sub_namespace, callbacks in _SUBSCRIBERS.items():
            if self._matches(namespace, sub_namespace):
                for callback in callbacks:
                    callback(**kwargs)

    @staticmethod
    def _matches(event_namespace: str, subscriber_namespace: str) -> bool:
        """
        Check if an event namespace matches a subscriber namespace.

        Args:
            event_namespace (str): The namespace where event was emitted.
            subscriber_namespace (str): The namespace a subscriber registered
                for.

        Returns:
            bool: True if subscriber should receive the event.
        """
        if event_namespace == subscriber_namespace:
            return True

        # Wildcard match - subscriber wants all events under a root
        if subscriber_namespace.endswith(".*"):
            root = subscriber_namespace[:-2]
            return event_namespace.startswith(root + ".")

        return False


# This is here to protect the _subscribers dict, creating a protective closure.
custom_module = Broker(sys.modules[__name__].__name__)
sys.modules[__name__] = custom_module


# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Required for static type checkers to accept these names as members of
# this module.
# -----------------------------------------------------------------------------


# noinspection PyUnusedLocal
def register_subscriber(namespace: str, callback: CALLBACK) -> None:
    """See docstring above..."""
    pass


# noinspection PyUnusedLocal
def unregister_subscriber(namespace: str, callback: CALLBACK) -> None:
    """See docstring above..."""
    pass


# noinspection PyUnusedLocal
def emit(namespace: str, **kwargs: Any) -> None:
    """See docstring above..."""
    pass
