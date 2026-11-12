"""Exactly-once execution for agent tool calls.

The problem this solves is small and expensive: an agent calls a tool
(a payment charge, a booking, a refund), the call times out, and the
agent retries -- without knowing whether the first attempt went
through. Retry blindly and you double-charge. Don't retry and the
customer's order never ships.

The wrapper in this module makes the retry safe:

* the caller passes an **idempotency key** (one per logical operation);
* the first call executes the tool and stores its result;
* a retry with the same key returns the stored result **without
  re-executing** the tool;
* concurrent calls with the same key collapse into a single execution
  -- the losers wait and get the winner's result;
* keys expire after a configurable TTL, after which the same key
  executes again;
* a call that raises stores nothing, so the key is not poisoned and a
  later retry can still execute;
* every call is recorded in an :class:`AuditLog` as ``executed``,
  ``replayed`` or ``failed``.

One honest limit: this is a client-side wrapper. It guarantees the
tool function runs at most once per key *for calls whose outcome it
can observe*. If the tool applies a side effect and then raises (the
charge went through, then the network died before the receipt came
back), no wrapper can un-charge it -- that case needs the downstream
system itself to accept the key (the way Stripe does). What this kit
does guarantee is that retries and duplicate dispatches never
re-execute a call that already produced a stored result.
"""

import functools
import threading
import time

from .stores import MemoryStore

__all__ = ["AuditLog", "idempotent", "wrap"]


class AuditLog:
    """An append-only record of what the wrapper did, and why.

    Each entry is a dict with ``key``, ``outcome`` (``executed``,
    ``replayed`` or ``failed``), ``at`` (epoch seconds), ``duration_ms``
    and ``error`` (a string, or ``None``). ``executed`` means the tool
    really ran; ``replayed`` means the stored result was returned
    without running it.
    """

    def __init__(self):
        self.entries = []
        self._lock = threading.Lock()

    def record(self, key, outcome, duration_ms, error=None):
        with self._lock:
            self.entries.append(
                {
                    "key": key,
                    "outcome": outcome,
                    "at": time.time(),
                    "duration_ms": round(duration_ms, 3),
                    "error": str(error) if error is not None else None,
                }
            )

    def executed(self):
        with self._lock:
            return [e for e in self.entries if e["outcome"] == "executed"]

    def replayed(self):
        with self._lock:
            return [e for e in self.entries if e["outcome"] == "replayed"]

    def failed(self):
        with self._lock:
            return [e for e in self.entries if e["outcome"] == "failed"]

    def __len__(self):
        with self._lock:
            return len(self.entries)


class _InFlight:
    """Bookkeeping for one key that is executing right now."""

    def __init__(self):
        self.done = threading.Event()


def _default_key(fn, args, kwargs):
    parts = [fn.__module__, fn.__qualname__, repr(args)]
    for name in sorted(kwargs):
        parts.append(f"{name}={kwargs[name]!r}")
    return "|".join(parts)


def idempotent(store=None, ttl_seconds=3600, key=None, clock=None, audit=None):
    """Wrap a tool function so retries with the same key are safe.

    ``store`` is where completed results live (a :class:`MemoryStore`
    is created when omitted). ``ttl_seconds`` is how long a stored
    result stays valid; after that the same key executes again.
    ``key`` is an optional function ``key(*args, **kwargs)`` that
    derives the idempotency key from the call -- when omitted, the key
    is derived from the function name and its arguments, unless the
    caller passes ``idempotency_key=...`` explicitly, which always
    wins. ``clock`` is an injectable ``() -> float`` time source
    (``time.time`` by default); ``audit`` is an :class:`AuditLog` to
    record into (one is created when omitted).
    """

    if store is None:
        store = MemoryStore()
    if clock is None:
        clock = time.time
    if audit is None:
        audit = AuditLog()

    def decorator(fn):
        state_lock = threading.Lock()
        in_flight = {}

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            kwargs = dict(kwargs)
            explicit_key = kwargs.pop("idempotency_key", None)
            if explicit_key is not None:
                call_key = str(explicit_key)
            elif key is not None:
                call_key = str(key(*args, **kwargs))
            else:
                call_key = _default_key(fn, args, kwargs)

            while True:
                entry = store.get(call_key)
                if entry is not None:
                    if clock() - entry["stored_at"] < ttl_seconds:
                        audit.record(call_key, "replayed", 0.0)
                        return entry["result"]
                    store.delete(call_key)

                with state_lock:
                    flight = in_flight.get(call_key)
                    if flight is None:
                        flight = _InFlight()
                        in_flight[call_key] = flight
                        leader = True
                    else:
                        leader = False

                if not leader:
                    flight.done.wait()
                    continue

                started = clock()
                try:
                    result = fn(*args, **kwargs)
                except Exception as exc:
                    audit.record(call_key, "failed", (clock() - started) * 1000, exc)
                    with state_lock:
                        in_flight.pop(call_key, None)
                    flight.done.set()
                    raise
                duration_ms = (clock() - started) * 1000
                store.set(call_key, {"result": result, "stored_at": clock()})
                audit.record(call_key, "executed", duration_ms)
                with state_lock:
                    in_flight.pop(call_key, None)
                flight.done.set()
                return result

        wrapper.idempotency_store = store
        wrapper.idempotency_audit = audit
        return wrapper

    return decorator


def wrap(fn, store=None, ttl_seconds=3600, key=None, clock=None, audit=None):
    """Wrap ``fn`` with idempotency without using decorator syntax."""
    return idempotent(store, ttl_seconds, key, clock, audit)(fn)
