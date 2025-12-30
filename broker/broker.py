"""
# Primary Event Broker

Herein is the event broker system itself as a module class to create a
protective closure around the subscriber namespace table.

Function stubs exist at the bottom of the file for static type checkers to
validate correct calls during CI/CD.
"""

import asyncio
import inspect
import json
import sys
import weakref
from dataclasses import dataclass
from types import ModuleType
from typing import Any
from typing import Callable
from typing import Coroutine
from typing import Optional
from typing import Union


class SignatureMismatchError(Exception):
    """Raised when callback signatures don't match for a namespace."""


class EmitArgumentError(Exception):
    """Raised when emit arguments don't match subscriber signatures."""


CALLBACK = Union[Callable[..., Any], Callable[..., Coroutine[Any, Any, Any]]]
"""
The callback end point that event info is forwarded to. These are the actions
that 'subscribe' and will execute when an event is triggered. Can be sync or
async.

The broker cannot determine which value to send back to the caller.
If you want data back, create an event going the opposite direction.
"""


@dataclass(frozen=True)
class Subscriber(object):
    """A subscriber with a callback and priority."""

    weak_callback: Union[weakref.ref[Any], weakref.WeakMethod]
    """
    The end point that data is forwarded to. i.e. what gets ran.
    This is a weak reference so the callback isn't kept alive by the broker.
    Broker can notify when item is garbage collected.
    """

    priority: int
    """Where in the execution order the callback should take place."""

    is_async: bool
    """If the item is asynchronous or not..."""

    namespace: str
    """The namespace the subscriber is listening to."""

    @property
    def callback(self) -> Optional[CALLBACK]:
        """Get the live callback, or None if collected."""
        return self.weak_callback()


_SUBSCRIBERS: dict[str, list[Subscriber]] = {}
"""
The broker's record of each namespace to subscribers.

This is kept outside of the replaced module class to create a protected
closure around the event namespace:subscriber structure.
"""

_NAMESPACE_SIGNATURES: dict[str, Optional[set[str]]] = {}
"""Track the expected keyword arguments for each namespace."""

_NOTIFY_NAMESPACE_ROOT = "broker.notify."
BROKER_ON_SUBSCRIBER_ADDED = f"{_NOTIFY_NAMESPACE_ROOT}subscriber.added"
BROKER_ON_SUBSCRIBER_REMOVED = f"{_NOTIFY_NAMESPACE_ROOT}subscriber.removed"
BROKER_ON_SUBSCRIBER_COLLECTED = f"{_NOTIFY_NAMESPACE_ROOT}subscriber.collected"
BROKER_ON_EMIT = f"{_NOTIFY_NAMESPACE_ROOT}emit.sync"
BROKER_ON_EMIT_ASYNC = f"{_NOTIFY_NAMESPACE_ROOT}emit.async"
BROKER_ON_EMIT_ALL = f"{_NOTIFY_NAMESPACE_ROOT}emit.all"
BROKER_ON_NAMESPACE_CREATED = f"{_NOTIFY_NAMESPACE_ROOT}namespace.created"
BROKER_ON_NAMESPACE_DELETED = f"{_NOTIFY_NAMESPACE_ROOT}namespace.deleted"


def _make_weak_ref(
    callback: CALLBACK, namespace: str, on_collected_callback: Callable[[str], None]
) -> Union[weakref.ref[Any], weakref.WeakMethod]:
    """Create appropriate weak reference for any callback type."""

    def cleanup(_: Union[weakref.ref[Any], weakref.WeakMethod]) -> None:
        # Arg needed to add for weakref creation.
        on_collected_callback(namespace)

    if hasattr(callback, "__self__"):
        return weakref.WeakMethod(callback, cleanup)
    else:
        return weakref.ref(callback, cleanup)


def _make_subscribe_decorator(broker_module: "Broker") -> Callable:
    """
    Create a subscribe decorator with access to the broker module.

    This exists as a function accepting the broker module as an argument so the
    function can call register_subscriber() on the broker without referring to
    it using a python namespace and thus creating a circular reference.
    """

    def subscribe_(namespace: str, priority: int = 0) -> CALLBACK:
        """
        Decorator to register a function or static method as a subscriber.

        To register an instance referencing class method (one using 'self'),
        use broker.register_subscriber('source', 'event_name', self.method).

        Usage:
            @subscribe('system.file.io', 5)
            def on_file_open(filepath: str) -> None:
                print(f'File opened: {filepath}')
        Args:
            namespace (str): The event namespace to subscribe to.
            priority (int): The execution priority. Defaults to 0.
        Returns:
            Callable: Decorator function that registers the subscriber.
        """

        def decorator(func: CALLBACK) -> CALLBACK:
            broker_module.register_subscriber(namespace, func, priority)
            return func

        return decorator

    return subscribe_


class Broker(ModuleType):
    """
    Primary event coordinator.
    Supports hierarchical namespace through dot notation, with * for wildcards.

    Supports both synchronous and asynchronous subscribers.
    Use emit() for fire-and-forget behavior.
    Use emit_async() to await all subscribers.
    """

    # -----Runtime Closures-----
    CALLBACK = CALLBACK
    SignatureMismatchError = SignatureMismatchError
    EmitArgumentError = EmitArgumentError

    BROKER_ON_SUBSCRIBER_ADDED = BROKER_ON_SUBSCRIBER_ADDED
    BROKER_ON_SUBSCRIBER_REMOVED = BROKER_ON_SUBSCRIBER_REMOVED
    BROKER_ON_SUBSCRIBER_COLLECTED = BROKER_ON_SUBSCRIBER_COLLECTED
    BROKER_ON_EMIT = BROKER_ON_EMIT
    BROKER_ON_EMIT_ASYNC = BROKER_ON_EMIT_ASYNC
    BROKER_ON_EMIT_ALL = BROKER_ON_EMIT_ALL
    BROKER_ON_NAMESPACE_CREATED = BROKER_ON_NAMESPACE_CREATED
    BROKER_ON_NAMESPACE_DELETED = BROKER_ON_NAMESPACE_DELETED

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.subscribe = _make_subscribe_decorator(self)

        # -----Notifies-----
        self.notify_on_all: bool = False
        self.notify_on_subscribe: bool = False
        self.notify_on_unsubscribe: bool = False
        self.notify_on_collected: bool = False
        self.notify_on_emit: bool = False
        self.notify_on_emit_async: bool = False
        self.notify_on_emit_all: bool = False
        self.notify_on_new_namespace: bool = False
        self.notify_on_del_namespace: bool = False

    @staticmethod
    def clear() -> None:
        _SUBSCRIBERS.clear()
        _NAMESPACE_SIGNATURES.clear()

    @staticmethod
    def _get_callback_params(callback: CALLBACK) -> Union[set[str], None]:
        """
        Extract parameter names from a callback function.

        Args:
            callback (CALLBACK): The callback function to inspect.
        Returns:
            Union[set[str], None]: Set of parameter names, or None if callback
                accepts **kwargs.
        """
        sig = inspect.signature(callback)

        # **kwargs is not tracked
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                return None

        return {
            name
            for name, param in sig.parameters.items()
            if param.kind != inspect.Parameter.VAR_POSITIONAL  # exclude *args
        }

    def _on_callback_collected(self, namespace: str) -> None:
        """Called when a subscriber is garbage collected."""
        if namespace in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = [
                sub for sub in _SUBSCRIBERS[namespace] if sub.callback is not None
            ]

        if self.notify_on_collected and not namespace.startswith(
            _NOTIFY_NAMESPACE_ROOT
        ):
            self.emit(namespace=BROKER_ON_SUBSCRIBER_COLLECTED, using=namespace)

    def register_subscriber(
        self, namespace: str, callback: CALLBACK, priority: int = 0
    ) -> None:
        """
        Register a callback function to a namespace.

        Args:
            namespace (str): Event namespace
                (e.g., 'system.io.file_open' or 'system.*').
            callback (CALLBACK): Function to call when events are emitted. Can
                be sync or async.
            priority (int): The priority used for callback execution order.
                Higher priorities are ran before lower priorities.
        Raises:
            SignatureMismatchError: If callback signature doesn't match
                existing subscribers.
        Notes:
            Emits a notify event when a namespace is created and when a
            subscriber is registered. Notify emits the used namespace.
        """
        callback_params = Broker._get_callback_params(callback)
        is_async = asyncio.iscoroutinefunction(callback)
        weak_callback = _make_weak_ref(callback, namespace, self._on_callback_collected)
        subscriber = Subscriber(
            weak_callback=weak_callback,
            priority=priority,
            is_async=is_async,
            namespace=namespace,
        )

        # If this is the first subscriber for this namespace, store signature
        if namespace not in _NAMESPACE_SIGNATURES:
            _NAMESPACE_SIGNATURES[namespace] = callback_params
        else:
            existing_params = _NAMESPACE_SIGNATURES[namespace]

            # If either accepts **kwargs, they're compatible
            if existing_params is None or callback_params is None:
                _NAMESPACE_SIGNATURES[namespace] = None
            elif existing_params != callback_params:
                raise SignatureMismatchError(
                    f"Callback parameter mismatch for namespace '{namespace}'. "
                    f"Expected parameters: {sorted(existing_params)}, "
                    f"but got: {sorted(callback_params)}"
                )

        if namespace not in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = []
            if (
                not namespace.startswith(_NOTIFY_NAMESPACE_ROOT)
                and self.notify_on_new_namespace
            ):
                self.emit(namespace=BROKER_ON_NAMESPACE_CREATED, using=namespace)

        _SUBSCRIBERS[namespace].append(subscriber)
        if (
            not namespace.startswith(_NOTIFY_NAMESPACE_ROOT)
            and self.notify_on_subscribe
        ):
            self.emit(namespace=BROKER_ON_SUBSCRIBER_ADDED, using=namespace)

    def unregister_subscriber(self, namespace: str, callback: CALLBACK) -> None:
        """
        Remove a callback from a namespace.

        Args:
            namespace (str): Event namespace.
            callback (CALLBACK): Function to remove.
        Notes:
            Emits a notify event when subscriber is unregistered and when a
            namespace is removed from consolidation. Notify emits the used
            namespace.
        """
        if namespace in _SUBSCRIBERS:
            _SUBSCRIBERS[namespace] = [
                sub for sub in _SUBSCRIBERS[namespace] if sub.callback != callback
            ]
            if (
                not namespace.startswith(_NOTIFY_NAMESPACE_ROOT)
                and self.notify_on_unsubscribe
            ):
                self.emit(namespace=BROKER_ON_SUBSCRIBER_REMOVED, using=namespace)

            if not _SUBSCRIBERS[namespace]:
                del _SUBSCRIBERS[namespace]

                # Clean up signature tracking if no subscribers left
                if namespace in _NAMESPACE_SIGNATURES:
                    del _NAMESPACE_SIGNATURES[namespace]

                if (
                    not namespace.startswith(_NOTIFY_NAMESPACE_ROOT)
                    and self.notify_on_del_namespace
                ):
                    self.emit(namespace=BROKER_ON_NAMESPACE_DELETED, using=namespace)

    @staticmethod
    def _validate_emit_args(namespace: str, kwargs: dict[str, Any]) -> None:
        """
        Validate that emit arguments match subscriber signatures.

        Args:
            namespace (str): The namespace being emitted to.
            kwargs (dict[str, Any]): The keyword arguments being emitted.
        Raises:
            EmitArgumentError: If provided kwargs don't match subscriber signatures.
        """
        provided_args = set(kwargs.keys())

        matching_namespaces = []
        for sub_namespace in _SUBSCRIBERS.keys():
            if Broker._matches(namespace, sub_namespace):
                matching_namespaces.append(sub_namespace)

        for sub_namespace in matching_namespaces:
            expected_params = _NAMESPACE_SIGNATURES.get(sub_namespace)

            if expected_params is None:  # **kwargs not validated
                continue

            if provided_args != expected_params:
                raise EmitArgumentError(
                    f"Argument mismatch when emitting to '{namespace}'. "
                    f"Subscribers in '{sub_namespace}' expect: {sorted(expected_params)}, "
                    f"but got: {sorted(provided_args)}"
                )

    def emit(self, namespace: str, **kwargs: Any) -> None:
        """
        Emit an event to all matching synchronous subscribers.

        Synchronous subscribers are called immediately in priority order.
        Asynchronous subscribers are NOT called - they are skipped entirely.

        Use emit_async() if you need to call async subscribers or await their
        completion.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open').
            **kwargs (Any): Arguments to pass to subscriber callbacks.
        Raises:
            EmitArgumentError: If provided kwargs don't match subscriber signatures.
        Note:
            -This method only calls synchronous callbacks. Async callbacks are
            skipped. Use emit_async() to call async callbacks.
            -Emits a notify event after args have been sent to subscribers.
            Notify emits the used namespace.
        """
        self._validate_emit_args(namespace, kwargs)

        for sub_namespace, subscribers in _SUBSCRIBERS.items():
            if self._matches(namespace, sub_namespace):
                sorted_subscribers = sorted(
                    subscribers, key=lambda s: s.priority, reverse=True
                )
                for subscriber in sorted_subscribers:
                    if not subscriber.is_async:  # Only call sync callbacks
                        subscriber.callback(**kwargs)

        if not namespace.startswith(_NOTIFY_NAMESPACE_ROOT) and (
            self.notify_on_emit or self.notify_on_emit_all
        ):
            self.emit(namespace=BROKER_ON_EMIT, using=namespace)

    async def emit_async(self, namespace: str, **kwargs: Any) -> None:
        """
        Asynchronously emit an event to all matching subscribers.

        Both synchronous and asynchronous subscribers are called in priority order.
        - Synchronous subscribers are executed immediately.
        - Asynchronous subscribers are awaited sequentially.

        This method must be awaited. Execution blocks until all subscribers complete.
        Use emit() for fire-and-forget behavior with sync-only subscribers.

        Args:
            namespace (str): Event namespace (e.g., 'system.io.file_open').
            **kwargs (Any): Arguments to pass to subscriber callbacks.
        Raises:
            EmitArgumentError: If provided kwargs don't match subscriber
                signatures.
        Note:
            -This method calls both sync and async callbacks. Sync callbacks are
            executed normally, async callbacks are awaited.
            -Emits a notify event after args have been sent to subscribers.
            Notify emits the used namespace.
        """
        self._validate_emit_args(namespace, kwargs)

        for sub_namespace, subscribers in _SUBSCRIBERS.items():
            if self._matches(namespace, sub_namespace):
                sorted_subscribers = sorted(
                    subscribers, key=lambda s: s.priority, reverse=True
                )
                for subscriber in sorted_subscribers:
                    if subscriber.is_async:
                        await subscriber.callback(**kwargs)
                    else:
                        subscriber.callback(**kwargs)

        if not namespace.startswith(_NOTIFY_NAMESPACE_ROOT) and (
            self.notify_on_emit_async or self.notify_on_emit_all
        ):
            self.emit(namespace=BROKER_ON_EMIT_ASYNC, using=namespace)

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
            # Although not strictly necessary to remove . and *, doing so adds
            # slightly more validity to the check.
            root = subscriber_namespace[:-2]
            return event_namespace.startswith(root + ".")

        return False

    def set_flag_sates(
        self,
        on_subscribe: bool = False,
        on_unsubscribe: bool = False,
        on_collected: bool = False,
        on_emit: bool = False,
        on_emit_async: bool = False,
        on_emit_all: bool = False,
        on_new_namespace: bool = False,
        on_del_namespace: bool = False,
    ) -> None:
        """
        Set the notification flags on or off for each type of broker activity.
        The broker can be configured through any of the following:

        Args:
            on_subscribe:    	if True, get notified whenever register_subscriber() is called;
            on_unsubscribe:  	if True, get notified whenever unregister_subscriber() is called;
            on_collected:       if True, get notified whenever a subscriber has been garbage collected;
            on_emit:			if True, get notified whenever emit() is called;
            on_emit_async:		if True, get notified whenever emit_async() is called;
            on_emit_all:		if True, get notified whenever emit() or emit_async() is called.
            on_new_namespace: 	if True, get notified whenever a new namespace is created;
            on_del_namespace:	if True, get notified whenever a namespace is "deleted";
        """
        self.notify_on_subscribe = on_subscribe
        self.notify_on_unsubscribe = on_unsubscribe
        self.notify_on_collected = on_collected
        self.notify_on_emit = on_emit
        self.notify_on_emit_async = on_emit_async
        self.notify_on_emit_all = on_emit_all
        self.notify_on_new_namespace = on_new_namespace
        self.notify_on_del_namespace = on_del_namespace

    @staticmethod
    def to_string() -> str:
        """Returns a string representation of the broker."""
        keys = sorted(_SUBSCRIBERS.keys())
        data = {}

        for namespace in keys:
            subscribers_info = []
            for sub in _SUBSCRIBERS[namespace]:
                # Get live callback (or None if collected)
                callback = sub.callback

                if callback is None:
                    info = "<dead reference>"

                elif hasattr(callback, "__self__"):
                    obj = callback.__self__
                    class_name = obj.__class__.__name__
                    method_name = callback.__name__
                    info = f"{class_name}.{method_name}"

                elif hasattr(callback, "__qualname__"):
                    # Regular function, static method, or class method
                    module = getattr(callback, "__module__", "<unknown>")
                    qualname = callback.__qualname__
                    info = f"{module}.{qualname}"

                else:
                    # Fallback for unusual callables
                    info = str(callback)

                # Priority and async info
                priority_str = (
                    f" [priority={sub.priority}]" if sub.priority != 0 else ""
                )
                async_str = " [async]" if sub.is_async else ""
                subscribers_info.append(f"{info}{priority_str}{async_str}")

            data[namespace] = subscribers_info

        return json.dumps(data, indent=4)


# This is here to protect the _SUBSCRIBERS dict, creating a protective closure.
custom_module = Broker(sys.modules[__name__].__name__)
sys.modules[__name__] = custom_module


# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# Required for static type checkers to accept these names as members of
# this module.
# -----------------------------------------------------------------------------


def clear() -> None:
    """See docstring above..."""


# noinspection PyUnusedLocal
def register_subscriber(namespace: str, callback: CALLBACK, priority: int = 0) -> None:
    """See docstring above..."""


# noinspection PyUnusedLocal
def unregister_subscriber(namespace: str, callback: CALLBACK) -> None:
    """See docstring above..."""


# noinspection PyUnusedLocal
def emit(namespace: str, **kwargs: Any) -> None:
    """See docstring above..."""


# noinspection PyUnusedLocal
async def emit_async(namespace: str, **kwargs: Any) -> None:
    """See docstring above..."""


# noinspection PyUnusedLocal
def subscribe(namespace: str, priority: int = 0) -> CALLBACK:
    """See docstring for subscribe_ above..."""


# noinspection PyUnusedLocal
def set_flag_sates(
    on_subscribe: bool = False,
    on_unsubscribe: bool = False,
    on_collected: bool = False,
    on_emit: bool = False,
    on_emit_async: bool = False,
    on_emit_all: bool = False,
    on_new_namespace: bool = False,
    on_del_namespace: bool = False,
) -> None:
    """See docstring above..."""


def to_string() -> str:
    """See docstring above..."""
