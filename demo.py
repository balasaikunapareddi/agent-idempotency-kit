"""Demo: the double-charge, and how the kit stops it.

Run it with:  python demo.py

The story: an agent charges a customer $24.50 for order A-103.
The payment gateway is flaky -- the first call times out. The agent
retries, because that is what agents do.

Part 1 shows what happens with no protection: the retry (and the
re-run after the agent loses the receipt) charges the customer twice.

Part 2 shows the same flaky gateway behind the kit: the first call
fails before anything is charged, the retry executes once and stores
the receipt, and the next retry replays the stored receipt. One
charge, ever, for that idempotency key.

Part 3 fires five identical calls at the same time. They collapse
into one execution.

Part 4 shows the file-backed store: after a "restart" (a brand-new
store object on the same file) the same key still replays instead of
charging again.

Everything here is scripted and deterministic -- the gateway fails
on its first call for each order on purpose, so the output is the
same every run.
"""

import os
import tempfile
import threading
import time

from idempotency_kit import FileStore, MemoryStore, idempotent


class Ledger:
    """The merchant's record of money actually taken."""

    def __init__(self):
        self.charges = []

    def charge(self, order_id, amount):
        receipt = {
            "order_id": order_id,
            "amount": amount,
            "receipt_id": f"rcpt-{order_id}-{len(self.charges) + 1}",
        }
        self.charges.append(receipt)
        return receipt

    def total_charged(self, order_id):
        return sum(c["amount"] for c in self.charges if c["order_id"] == order_id)


class FlakyGateway:
    """A payment gateway that times out on its first call per order.

    The failure happens *before* any money moves -- the request never
    reached the processor. That is the case a client-side wrapper can
    fix: the call raised, so nothing was stored, and a retry is safe.
    """

    def __init__(self, ledger):
        self.ledger = ledger
        self._failed_once = set()

    def charge(self, order_id, amount):
        if order_id not in self._failed_once:
            self._failed_once.add(order_id)
            raise TimeoutError("gateway timed out -- response lost")
        time.sleep(0.05)  # slow enough that concurrent calls overlap
        return self.ledger.charge(order_id, amount)


def part1_no_protection():
    print("PART 1 -- no idempotency: the agent retries a timed-out charge")
    print("-" * 70)
    ledger = Ledger()
    gateway = FlakyGateway(ledger)

    try:
        gateway.charge("A-103", 24.50)
    except TimeoutError as exc:
        print(f"  attempt 1: {exc} (no charge applied)")

    # The agent retries. It succeeds -- but the agent crashes before
    # it records the receipt, so as far as it knows, the charge is
    # still unconfirmed. It retries once more.
    gateway.charge("A-103", 24.50)
    print("  attempt 2: charged $24.50, but the receipt was lost (agent crashed)")
    gateway.charge("A-103", 24.50)
    print("  attempt 3: agent retries -- charged $24.50 AGAIN")

    total = ledger.total_charged("A-103")
    print(f"  => customer was charged ${total:.2f} for a $24.50 order "
          f"({len(ledger.charges)} charges)")
    print()
    return total


def part2_with_kit():
    print("PART 2 -- with the kit: the same flaky gateway, one charge")
    print("-" * 70)
    ledger = Ledger()
    gateway = FlakyGateway(ledger)
    store = MemoryStore()

    @idempotent(store=store, ttl_seconds=3600)
    def charge(order_id, amount):
        return gateway.charge(order_id, amount)

    try:
        charge("A-103", 24.50, idempotency_key="order-A-103-charge-1")
    except TimeoutError as exc:
        print(f"  attempt 1: {exc} (nothing stored -- the key is not poisoned)")

    receipt = charge("A-103", 24.50, idempotency_key="order-A-103-charge-1")
    print(f"  attempt 2: executed once, receipt {receipt['receipt_id']}")
    replayed = charge("A-103", 24.50, idempotency_key="order-A-103-charge-1")
    print(f"  attempt 3: replayed stored receipt {replayed['receipt_id']} "
          f"-- tool did not run again")

    total = ledger.total_charged("A-103")
    print(f"  => customer was charged ${total:.2f} for a $24.50 order "
          f"({len(ledger.charges)} charge)")
    print(f"  audit: {len(charge.idempotency_audit.executed())} executed, "
          f"{len(charge.idempotency_audit.replayed())} replayed, "
          f"{len(charge.idempotency_audit.failed())} failed")
    print()
    return total


def part3_concurrent_duplicates():
    print("PART 3 -- five identical calls at once collapse into one execution")
    print("-" * 70)
    ledger = Ledger()
    gateway = FlakyGateway(ledger)
    # The gateway's scripted first-call failure would fire inside one
    # of the threads; use a gateway that is past its failure.
    gateway._failed_once.add("A-104")
    store = MemoryStore()

    @idempotent(store=store, ttl_seconds=3600)
    def charge(order_id, amount):
        return gateway.charge(order_id, amount)

    results = []
    barrier = threading.Barrier(5)

    def worker():
        barrier.wait()
        results.append(charge("A-104", 24.50, idempotency_key="order-A-104-charge-1"))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    receipts = {r["receipt_id"] for r in results}
    print(f"  5 threads, {len(receipts)} distinct receipt(s), "
          f"{len(ledger.charges)} charge(s) applied")
    print(f"  audit: {len(charge.idempotency_audit.executed())} executed, "
          f"{len(charge.idempotency_audit.replayed())} replayed")
    print()
    return len(ledger.charges)


def part4_file_store_survives_restart():
    print("PART 4 -- file store: the result survives a process restart")
    print("-" * 70)
    ledger = Ledger()
    gateway = FlakyGateway(ledger)
    gateway._failed_once.add("A-105")
    path = os.path.join(tempfile.mkdtemp(), "charges.json")

    @idempotent(store=FileStore(path), ttl_seconds=3600)
    def charge(order_id, amount):
        return gateway.charge(order_id, amount)

    receipt = charge("A-105", 24.50, idempotency_key="order-A-105-charge-1")
    print(f"  before restart: executed, receipt {receipt['receipt_id']}")

    # "Restart": a brand-new store object reading the same file.
    @idempotent(store=FileStore(path), ttl_seconds=3600)
    def charge_after_restart(order_id, amount):
        return gateway.charge(order_id, amount)

    replayed = charge_after_restart("A-105", 24.50,
                                    idempotency_key="order-A-105-charge-1")
    print(f"  after restart: replayed stored receipt {replayed['receipt_id']} "
          f"-- tool did not run again")
    print(f"  => {len(ledger.charges)} charge(s) applied in total")
    print()
    return len(ledger.charges)


def main():
    print("agent-idempotency-kit demo")
    print("=" * 70)
    print()
    without = part1_no_protection()
    with_kit = part2_with_kit()
    concurrent = part3_concurrent_duplicates()
    restarted = part4_file_store_survives_restart()

    print("SUMMARY")
    print("=" * 70)
    print(f"  without the kit:  ${without:.2f} charged for a $24.50 order (double charge)")
    print(f"  with the kit:     ${with_kit:.2f} charged (exactly once)")
    print(f"  5 concurrent duplicates: {concurrent} execution")
    print(f"  across a restart: {restarted} execution")
    print()
    assert without == 49.00 and with_kit == 24.50
    assert concurrent == 1 and restarted == 1
    print("All demo assertions passed.")


if __name__ == "__main__":
    main()
