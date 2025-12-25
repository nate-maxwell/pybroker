import sys
from typing import Any
from typing import Callable
from types import ModuleType


_subscribers: dict[str, list[Callable]] = {}


class Broker(ModuleType):
    """Message broker system with hierarchical namespaces."""

    def __init__(self, name: str) -> None:
        super().__init__(name)

    @staticmethod
    def register_subscriber(namespace: str, callback: Callable) -> None:
        """
        Register a callback function to a namespace.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open' or
                'system.*').
            callback (Callable): Function to call when events are emitted.
        """
        if namespace not in _subscribers:
            _subscribers[namespace] = []
        _subscribers[namespace].append(callback)

    @staticmethod
    def unregister_subscriber(namespace: str, callback: Callable) -> None:
        """
        Remove a callback from a namespace.

        Args:
            namespace (str): Event namespace.
            callback (Callable): Function to remove.
        """
        if namespace in _subscribers:
            _subscribers[namespace].remove(callback)
            if not _subscribers[namespace]:
                del _subscribers[namespace]

    def emit(self, namespace: str, **kwargs: Any) -> None:
        """
        Emit an event to all matching subscribers.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open').
            **kwargs: Arguments to pass to subscriber callbacks.
        """
        for sub_namespace, callbacks in _subscribers.items():
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
        if subscriber_namespace.endswith('.*'):
            root = subscriber_namespace[:-2]
            return event_namespace.startswith(root + '.')

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
def register_subscriber(namespace: str, callback: Callable) -> None:
    pass


# noinspection PyUnusedLocal
def unregister_subscriber(namespace: str, callback: Callable) -> None:
    pass


# noinspection PyUnusedLocal
def emit(namespace: str, **kwargs: Any) -> None:
    pass
