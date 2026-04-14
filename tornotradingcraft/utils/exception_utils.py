"""Exception handling helpers for the tornotradingcraft package.

Centralized implementations so callers across the repo can import from
`tornotradingcraft.utils.exception_utils` instead of duplicating logic.
"""
import functools
import logging

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


def swallow_exceptions(default=None, log_exc: bool = False):
    """Decorator that swallows exceptions and optionally logs them.

    Use this on small callback functions or methods that must not propagate
    exceptions back into a caller (for example, API threads). Returns
    `default` when an exception is caught (default: None).
    """
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                if log_exc:
                    log.exception("Suppressed exception in %s", func.__name__)
                return default

        return wrapper

    return deco


def safe_call(func, *args, **kwargs):
    """Call a callable and swallow any exception, returning None on failure.

    Useful at call sites where a best-effort operation should not raise.
    """
    try:
        return func(*args, **kwargs)
    except Exception:
        return None


def expect_exception(raise_exc: type, message: str = None, except_types: tuple = (Exception,)):
    """Decorator that converts caught exceptions into a specific exception type.

    - raise_exc: exception class to raise (e.g. BrokerError)
    - message: optional format string. It will be formatted with the
      function's bound arguments plus an `exc` variable containing the
      original exception. Example: "Failed to cancel order {order_id}: {exc}"
    - except_types: tuple of exception types to catch (defaults to Exception)

    The decorator will re-raise exceptions of type `raise_exc` unchanged so
    callers that intentionally raise the target type are preserved.
    """
    import inspect

    def deco(func):
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except except_types as exc:
                # if it's already the target type, propagate
                if isinstance(exc, raise_exc):
                    raise

                # bind args for message formatting
                try:
                    bound = sig.bind_partial(*args, **kwargs)
                    ctx = dict(bound.arguments)
                except Exception:
                    ctx = {}
                ctx["exc"] = exc

                if message:
                    try:
                        msg = message.format(**ctx)
                    except Exception:
                        msg = str(exc)
                else:
                    msg = str(exc)

                raise raise_exc(msg) from exc

        return wrapper

    return deco


def convert_exceptions(mapping: dict):
    """Decorator that maps raised exceptions to other exception types.

    mapping: dict where keys are source exception classes (or tuple of classes)
    and values are either target exception classes or (target_exc, msg_fmt)
    where msg_fmt is a format string that may reference `exc` and function args.
    """
    import inspect

    def deco(func):
        sig = inspect.signature(func)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                for src, tgt in mapping.items():
                    if isinstance(exc, src if isinstance(src, tuple) else (src,)):
                        # tgt may be class or (class, msg)
                        if isinstance(tgt, tuple):
                            tgt_cls, msg = tgt
                        else:
                            tgt_cls, msg = tgt, None

                        # create context for formatting
                        try:
                            bound = sig.bind_partial(*args, **kwargs)
                            ctx = dict(bound.arguments)
                        except Exception:
                            ctx = {}
                        ctx["exc"] = exc

                        if msg:
                            try:
                                final_msg = msg.format(**ctx)
                            except Exception:
                                final_msg = str(exc)
                        else:
                            final_msg = str(exc)

                        raise tgt_cls(final_msg) from exc

                # if no mapping matched, re-raise
                raise

        return wrapper

    return deco


def retry_on_exception(retries: int = 3, delay: float = 0.1, exceptions: tuple = (Exception,)):
    """Decorator that retries the function on specified exceptions.

    Example: @retry_on_exception(retries=3, delay=0.2, exceptions=(IOError,))
    """
    import time

    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt + 1 < retries:
                        time.sleep(delay)
                    else:
                        raise
            # should not reach here
            if last_exc:
                raise last_exc

        return wrapper

    return deco
