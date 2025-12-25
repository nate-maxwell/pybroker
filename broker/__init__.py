import sys
from typing import Any
from typing import Callable
from typing import NamedTuple
from types import ModuleType


CALLBACK = Callable[..., Any]
"""
The callback end point that event info is forwarded to. These are the actions
that 'subscribe' and will execute when an event is triggered. Can be sync or
async.

The broker cannot determine which value to send back to the caller.
If you want data back, create an event going the opposite direction.
"""


class Subscriber(NamedTuple):
    """A subscriber with a callback and priority."""

    callback: CALLBACK
    priority: int


_SUBSCRIBERS: dict[str, list[Subscriber]] = {}
"""The broker's record of each namespace to subscribers.

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
    def register_subscriber(
        namespace: str, callback: CALLBACK, priority: int = 0
    ) -> None:
        """
        Register a callback function to a namespace.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open' or
                'system.*').
            callback (Callable): Function to call when events are emitted.
            priority (int): The priority used for callback execution order.
                Higher priorities are ran before lower priorities.
        """
        if namespace not in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = []
        _SUBSCRIBERS[namespace].append(Subscriber(callback, priority))

    @staticmethod
    def unregister_subscriber(namespace: str, callback: CALLBACK) -> None:
        """
        Remove a callback from a namespace.

        Args:
            namespace (str): Event namespace.
            callback (Callable): Function to remove.
        """
        if namespace in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = [
                sub for sub in _SUBSCRIBERS[namespace] if sub.callback != callback
            ]
            if not _SUBSCRIBERS[namespace]:
                del _SUBSCRIBERS[namespace]

    def emit(self, namespace: str, **kwargs: Any) -> None:
        """
        Emit an event to all matching subscribers.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open').
            **kwargs: Arguments to pass to subscriber callbacks.
        """
        for sub_namespace, subscribers in _SUBSCRIBERS.items():
            if self._matches(namespace, sub_namespace):
                # Sort by priority (higher priority first)
                sorted_subscribers = sorted(
                    subscribers, key=lambda s: s.priority, reverse=True
                )
                for subscriber in sorted_subscribers:
                    subscriber.callback(**kwargs)

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


def clear() -> None:
    """See docstring above..."""
    pass


# noinspection PyUnusedLocal
def register_subscriber(namespace: str, callback: CALLBACK, priority: int = 0) -> None:
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
