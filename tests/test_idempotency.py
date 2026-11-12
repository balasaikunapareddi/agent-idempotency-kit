"""Tests for agent-idempotency-kit.

Run with:  python -m unittest discover -s tests -v

No third-party dependencies: everything here uses the standard library.
The fake clock makes TTL behaviour deterministic -- no test sleeps
for real time.
"""

import os
import tempfile
import threading
import unittest

from idempotency_kit import FileStore, MemoryStore, idempotent, wrap


class FakeClock:
    """A clock the test controls. Starts at an arbitrary epoch."""

    def __init__(self):
        self.now = 1_000_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class CountingTool:
    """A stand-in for a real tool. Counts executions, returns receipts."""

    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def run(self, order_id, amount):
        with self._lock:
            self.calls += 1
            n = self.calls
        return {"order_id": order_id, "amount": amount, "receipt_id": f"rcpt-{n}"}


class TestSameKeyReplays(unittest.TestCase):
    def test_same_key_replays_stored_result_without_reexecuting(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        first = guarded("A-1", 10.0, idempotency_key="k-1")
        second = guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(first, second)
        self.assertEqual(tool.calls, 1)

    def test_different_keys_execute_separately(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        guarded("A-1", 10.0, idempotency_key="k-1")
        guarded("A-2", 20.0, idempotency_key="k-2")
        guarded("A-1", 10.0, idempotency_key="k-1")  # replay, not a 3rd run
        self.assertEqual(tool.calls, 2)

    def test_key_derived_from_arguments_when_none_given(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        guarded("A-1", 10.0)
        guarded("A-1", 10.0)  # same args -> same derived key -> replay
        guarded("A-1", 15.0)  # different args -> different key -> execute
        self.assertEqual(tool.calls, 2)

    def test_key_function_derives_key_from_args(self):
        tool = CountingTool()
        guarded = wrap(
            tool.run,
            store=MemoryStore(),
            key=lambda order_id, amount: f"order:{order_id}",
        )
        guarded("A-1", 10.0)
        guarded("A-1", 99.0)  # same order, different amount: same key
        self.assertEqual(tool.calls, 1)


class TestConcurrency(unittest.TestCase):
    def test_concurrent_duplicates_execute_once(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        barrier = threading.Barrier(8)
        results = []

        def worker():
            barrier.wait()
            results.append(guarded("A-1", 10.0, idempotency_key="k-1"))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(tool.calls, 1)
        self.assertEqual(len({r["receipt_id"] for r in results}), 1)

    def test_concurrent_waiters_get_the_same_result(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        barrier = threading.Barrier(5)
        results = []

        def worker():
            barrier.wait()
            results.append(guarded("A-1", 10.0, idempotency_key="k-1"))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertTrue(all(r == results[0] for r in results))


class TestTTL(unittest.TestCase):
    def test_expired_key_reexecutes(self):
        clock = FakeClock()
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore(), ttl_seconds=60, clock=clock)
        guarded("A-1", 10.0, idempotency_key="k-1")
        guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(tool.calls, 1)
        clock.advance(61)  # past the TTL
        guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(tool.calls, 2)

    def test_unexpired_key_still_replays(self):
        clock = FakeClock()
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore(), ttl_seconds=60, clock=clock)
        guarded("A-1", 10.0, idempotency_key="k-1")
        clock.advance(59)  # just inside the TTL
        guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(tool.calls, 1)


class TestFailures(unittest.TestCase):
    def test_failure_before_storing_does_not_poison_the_key(self):
        attempts = {"n": 0}

        def flaky(order_id, amount):
            attempts["n"] += 1
            if attempts["n"] == 1:
                raise TimeoutError("gateway timed out")
            return {"order_id": order_id, "receipt_id": "rcpt-ok"}

        guarded = wrap(flaky, store=MemoryStore())
        with self.assertRaises(TimeoutError):
            guarded("A-1", 10.0, idempotency_key="k-1")
        # The retry with the same key must still be allowed to execute.
        result = guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(result["receipt_id"], "rcpt-ok")
        self.assertEqual(attempts["n"], 2)
        # And now the successful result replays.
        guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(attempts["n"], 2)

    def test_repeated_failures_keep_the_key_usable(self):
        attempts = {"n": 0}

        def always_fails(order_id, amount):
            attempts["n"] += 1
            raise ConnectionError("no route to gateway")

        guarded = wrap(always_fails, store=MemoryStore())
        for _ in range(3):
            with self.assertRaises(ConnectionError):
                guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(attempts["n"], 3)
        self.assertEqual(len(guarded.idempotency_audit.failed()), 3)


class TestAuditLog(unittest.TestCase):
    def test_audit_distinguishes_executed_vs_replayed(self):
        tool = CountingTool()
        guarded = wrap(tool.run, store=MemoryStore())
        guarded("A-1", 10.0, idempotency_key="k-1")
        guarded("A-1", 10.0, idempotency_key="k-1")
        guarded("A-2", 20.0, idempotency_key="k-2")

        audit = guarded.idempotency_audit
        self.assertEqual(len(audit.executed()), 2)
        self.assertEqual(len(audit.replayed()), 1)
        self.assertEqual(audit.replayed()[0]["key"], "k-1")
        self.assertEqual(
            {e["outcome"] for e in audit.entries},
            {"executed", "replayed"},
        )

    def test_audit_records_failures(self):
        def boom(order_id, amount):
            raise ValueError("bad amount")

        guarded = wrap(boom, store=MemoryStore())
        with self.assertRaises(ValueError):
            guarded("A-1", 10.0, idempotency_key="k-1")
        self.assertEqual(len(guarded.idempotency_audit.failed()), 1)
        self.assertIn("bad amount", guarded.idempotency_audit.failed()[0]["error"])


class TestFileStore(unittest.TestCase):
    def test_result_survives_a_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "store.json")
            tool = CountingTool()
            guarded = wrap(tool.run, store=FileStore(path))
            first = guarded("A-1", 10.0, idempotency_key="k-1")
            self.assertEqual(tool.calls, 1)

            # A brand-new store object on the same file: the result
            # must replay without executing the tool again.
            tool2 = CountingTool()
            guarded2 = wrap(tool2.run, store=FileStore(path))
            second = guarded2("A-1", 10.0, idempotency_key="k-1")
            self.assertEqual(first, second)
            self.assertEqual(tool2.calls, 0)

    def test_expired_file_entries_reexecute(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "store.json")
            clock = FakeClock()
            tool = CountingTool()
            guarded = wrap(
                tool.run, store=FileStore(path), ttl_seconds=30, clock=clock
            )
            guarded("A-1", 10.0, idempotency_key="k-1")
            clock.advance(31)
            guarded("A-1", 10.0, idempotency_key="k-1")
            self.assertEqual(tool.calls, 2)

    def test_non_json_result_raises_loudly(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "store.json")

            def returns_object(order_id, amount):
                return {"not": "serialisable", "obj": object()}

            guarded = wrap(returns_object, store=FileStore(path))
            with self.assertRaises(TypeError):
                guarded("A-1", 10.0, idempotency_key="k-1")


if __name__ == "__main__":
    unittest.main()
