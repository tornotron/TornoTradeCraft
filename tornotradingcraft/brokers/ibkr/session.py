"""Shared IBKR broker session helper.

This module provides a small, thread-safe shared broker helper useful for
interactive sessions (notebooks, REPL). It keeps a single `IBKRBroker`
instance and a reference count so multiple cells or scopes can acquire the
broker without reconnecting each time. When the last holder releases the
broker it will be disconnected.

Usage (notebook):
    # cell 1 - create/connect once
    from tornotradingcraft.brokers.ibkr.session import acquire_shared_broker
    b = acquire_shared_broker()

    # other cells - reuse `b`

    # final cell - when done
    from tornotradingcraft.brokers.ibkr.session import release_shared_broker
    release_shared_broker()

Or use the context manager to temporarily increment the refcount:
    from tornotradingcraft.brokers.ibkr.session import SharedIBKR
    with SharedIBKR() as b:
        ...
"""

import threading
from typing import Optional, Callable, Any
from functools import wraps
from .broker_ibkr import IBKRBroker

# Module-level shared state
_lock = threading.Lock()
_shared_broker: Optional[IBKRBroker] = None
_ref_count: int = 0


# Client ID used by the shared singleton broker. Not exposed to callers
# because this module manages exactly one connection.
_SHARED_CLIENT_ID = 1


def acquire_shared_broker(host: str = "127.0.0.1", port: int = 7497, connect_timeout: float = 10.0) -> IBKRBroker:
    """Acquire (and connect if needed) the shared IBKRBroker.

    The function is thread-safe. Each successful call increments an internal
    reference count; callers should call `release_shared_broker()` when they
    no longer need the broker.

    Returns the connected `IBKRBroker` instance.
    """
    global _shared_broker, _ref_count
    if IBKRBroker is None:
        raise ImportError("IBKRBroker cannot be used: import error from broker_ibkr: %s" % getattr(_import_err, "args", (_import_err,)))

    with _lock:
        if _shared_broker is None:
            b = IBKRBroker(host=host, port=port, client_id=_SHARED_CLIENT_ID, connect_timeout=connect_timeout)
            b.connect()
            _shared_broker = b
        _ref_count += 1
        return _shared_broker


def release_shared_broker() -> None:
    """Release one reference to the shared broker. When refcount reaches 0,
    the broker is disconnected and cleared.
    """
    global _shared_broker, _ref_count
    with _lock:
        if _ref_count <= 0:
            # nothing to do
            _ref_count = 0
            return
        _ref_count -= 1
        if _ref_count == 0 and _shared_broker is not None:
            try:
                _shared_broker.disconnect()
            finally:
                _shared_broker = None


def get_shared_broker() -> Optional[IBKRBroker]:
    """Return the current shared broker if present (may be None). Does NOT
    change the refcount or connect the broker.
    """
    global _shared_broker
    with _lock:
        return _shared_broker


def ensure_connected(func: Callable[..., Any] | None = None, *, host: str = "127.0.0.1", port: int = 7497,
                     connect_timeout: float = 10.0, inject_name: str = "broker"):
    """Decorator that acquires/releases the shared IBKR broker for the duration of the
    wrapped function call.

    Behaviour:
    - Calls `acquire_shared_broker()` before running the wrapped function.
    - Injects the acquired broker into the wrapped function as a keyword
      argument named by `inject_name` (default: 'broker') unless that keyword
      is already present in the call.
    - Always calls `release_shared_broker()` in a finally block so the
      reference count is decremented even if the wrapped function raises.

    Usage:
        @ensure_connected
        def my_task(..., broker=None):
            # use broker

        @ensure_connected(inject_name='ib')
        def my_task2(..., ib=None):
            # use ib
    """

    def _decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(f)
        def _wrapped(*args, **kwargs):
            b = acquire_shared_broker(host=host, port=port, connect_timeout=connect_timeout)
            try:
                # Inject broker if caller hasn't provided one explicitly
                if inject_name not in kwargs:
                    kwargs[inject_name] = b
                return f(*args, **kwargs)
            finally:
                # always release one reference
                release_shared_broker()

        return _wrapped

    # support both @ensure_connected and @ensure_connected(...)
    if callable(func):
        return _decorator(func)
    return _decorator


# alias with a clearer name for notebook users
with_shared_ibkr = ensure_connected


class SharedIBKR:
    """Context manager that acquires/releases the shared broker.

    Helpful for short-lived scopes or examples. In notebook usage prefer
    `acquire_shared_broker()` + `release_shared_broker()` across cells so the
    broker stays connected between cells.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, connect_timeout: float = 10.0):
        self._args = (host, port, connect_timeout)
        self.broker: Optional[IBKRBroker] = None

    def __enter__(self) -> IBKRBroker:
        self.broker = acquire_shared_broker(*self._args)
        return self.broker

    def __exit__(self, exc_type, exc, tb) -> None:
        # always release one ref even if exception occurred
        release_shared_broker()
