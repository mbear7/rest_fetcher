from __future__ import annotations

from collections.abc import Callable
from typing import Any

from .exceptions import CallbackError, RequestError, ResponseError


def safe_call(fn: Callable[..., Any] | None, *args: Any, name: str = 'callback') -> Any:
    """
    call an optional callback with uniform error semantics.

    - if fn is None, returns None
    - CallbackError, RequestError, and ResponseError are re-raised unchanged
    - any other exception is wrapped in CallbackError
    """
    if fn is None:
        return None
    try:
        return fn(*args)
    except (CallbackError, RequestError, ResponseError):
        raise
    except Exception as e:
        raise CallbackError(f'callback "{name}" raised: {e}', callback_name=name, cause=e) from e
