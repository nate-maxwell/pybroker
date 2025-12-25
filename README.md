# Broker

A simple message broker system for python.
Supports sync and async events.

## Namespaces

End-points can subscribe to hierarchical namespaces using dot notation like
`system.io.file_opened` or wildcard namespaces like `system.io.*`.

## Priorities

Subscriptions can add a priority integer that will dictate the subscriber
execution order:

```python
broker.subscribe('file.io.*', my_func, priority=10)
```

Higher priorities are executed first.

## Expressive Arguments

Events can be emitted with any keyword arguments:

```python
broker.emit('file.io.open_file', path=Path('D:/dir/file.txt'))
```
## Signature Validation

If a subscriber to a namespace with keyword arguments different from previous
subscribers, or an event is emitted using different keywords than subscribers
are expecting, an explicit exception is raised. First subscribers set the
expectation and following subscribers + emitters are validated.
