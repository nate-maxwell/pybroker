import broker


def test_notify_on_subscribe_flag() -> None:
    """
    Test that notify_on_subscribe flag triggers notifications when subscribers
    are added.
    """
    broker.clear()
    broker.set_flag_sates(on_subscribe=True)
    notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_SUBSCRIBER_ADDED)
    def on_subscriber_added(using: str) -> None:
        notifications.append(using)

    # noinspection PyUnusedLocal
    @broker.subscribe("test.event")
    def test_callback(data: str) -> None:
        pass

    assert "test.event" in notifications


def test_notify_on_unsubscribe_flag() -> None:
    """
    Test that notify_on_unsubscribe flag triggers notifications when
    subscribers are removed.
    """
    broker.clear()
    notifications: list[str] = []

    def on_subscriber_removed(using: str) -> None:
        print(f"Handler called with: {using}")
        notifications.append(using)

    broker.register_subscriber(
        broker.BROKER_ON_SUBSCRIBER_REMOVED, on_subscriber_removed
    )
    broker.set_flag_sates(on_unsubscribe=True)

    # noinspection PyUnusedLocal
    def test_callback(data: str, extra: str) -> None:
        pass

    broker.register_subscriber("test.event", test_callback)
    broker.unregister_subscriber("test.event", test_callback)

    assert "test.event" in notifications, f"Expected test.event in {notifications}"


def test_notify_on_emit_flag() -> None:
    """
    Test that notify_on_emit flag triggers notifications when emit() is called.
    """
    broker.clear()
    broker.set_flag_sates(on_emit=True)
    notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_EMIT)
    def on_emit(using: str) -> None:
        notifications.append(using)

    # noinspection PyUnusedLocal
    @broker.subscribe("test.event")
    def test_callback(data: str) -> None:
        pass

    broker.emit("test.event", data="test")

    assert "test.event" in notifications


def test_notify_on_new_namespace_flag() -> None:
    """
    Test that notify_on_new_namespace flag triggers when a new namespace is
    created.
    """
    broker.clear()
    broker.set_flag_sates(on_new_namespace=True)
    notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_NAMESPACE_CREATED)
    def on_namespace_created(using: str) -> None:
        notifications.append(using)

    # noinspection PyUnusedLocal
    @broker.subscribe("new.namespace")
    def test_callback(data: str) -> None:
        pass

    assert "new.namespace" in notifications


def test_notify_on_del_namespace_flag() -> None:
    """
    Test that notify_on_del_namespace flag triggers when a namespace is
    deleted.
    """
    broker.clear()
    notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_NAMESPACE_DELETED)
    def on_namespace_deleted(using: str) -> None:
        notifications.append(using)

    broker.set_flag_sates(on_del_namespace=True)

    # noinspection PyUnusedLocal
    def test_callback(data: str) -> None:
        pass

    broker.register_subscriber("temp.namespace", test_callback)
    broker.unregister_subscriber("temp.namespace", test_callback)

    assert "temp.namespace" in notifications


def test_notify_flags_off_by_default() -> None:
    """Test that all notify flags are off by default (no notifications sent)."""
    broker.clear()
    notifications: list[str] = []

    # noinspection PyUnusedLocal
    @broker.subscribe(broker.BROKER_ON_SUBSCRIBER_ADDED)
    def on_subscriber_added(using: str) -> None:
        notifications.append("subscribe")

    # noinspection PyUnusedLocal
    @broker.subscribe(broker.BROKER_ON_EMIT)
    def on_emit(using: str) -> None:
        notifications.append("emit")

    # noinspection PyUnusedLocal
    @broker.subscribe("test.event")
    def test_callback(data: str) -> None:
        pass

    broker.emit("test.event", data="test")

    assert len(notifications) == 0


def test_notify_does_not_trigger_for_notify_namespaces() -> None:
    """
    Test that broker notify namespaces don't trigger their own notifications
    (recursion guard).
    """
    broker.clear()
    broker.set_flag_sates(on_subscribe=True, on_emit=True)
    subscribe_notifications: list[str] = []
    emit_notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_SUBSCRIBER_ADDED)
    def on_subscriber_added(using: str) -> None:
        subscribe_notifications.append(using)

    @broker.subscribe(broker.BROKER_ON_EMIT)
    def on_emit(using: str) -> None:
        emit_notifications.append(using)

    # noinspection PyUnusedLocal
    def notify_callback(using: str) -> None:
        pass

    broker.register_subscriber(broker.BROKER_ON_NAMESPACE_CREATED, notify_callback)
    broker.emit(broker.BROKER_ON_NAMESPACE_CREATED, using="test")

    assert broker.BROKER_ON_NAMESPACE_CREATED not in subscribe_notifications
    assert broker.BROKER_ON_NAMESPACE_CREATED not in emit_notifications


def test_multiple_notify_flags_together() -> None:
    """Test that multiple notify flags can be enabled simultaneously."""
    broker.clear()
    broker.set_flag_sates(on_subscribe=True, on_new_namespace=True, on_emit=True)
    subscribe_notifications: list[str] = []
    namespace_notifications: list[str] = []
    emit_notifications: list[str] = []

    @broker.subscribe(broker.BROKER_ON_SUBSCRIBER_ADDED)
    def on_subscriber_added(using: str) -> None:
        subscribe_notifications.append(using)

    @broker.subscribe(broker.BROKER_ON_NAMESPACE_CREATED)
    def on_namespace_created(using: str) -> None:
        namespace_notifications.append(using)

    @broker.subscribe(broker.BROKER_ON_EMIT)
    def on_emit(using: str) -> None:
        emit_notifications.append(using)

    # noinspection PyUnusedLocal
    @broker.subscribe("test.multiple")
    def test_callback(data: str) -> None:
        pass

    broker.emit("test.multiple", data="test")

    assert "test.multiple" in subscribe_notifications
    assert "test.multiple" in namespace_notifications
    assert "test.multiple" in emit_notifications
