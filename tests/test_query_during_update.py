"""
Test Scenario: Query While Another User Is Updating

Purpose:
Ensure that a reader sees the original version of a record
when an uncommitted update is in progress.

Idea:
- Insert and commit records "A" and "B"
- Start txn3 to update both (uncommitted)
- In parallel, txn4 reads

Expected Result:
- Reader sees the committed versions of "A" and "B", not the uncommitted updates
"""

import sys
import os
import threading
import time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mvcc.store import Store
from mvcc.record import Record

def updater(store):
    tid = store.begin_transaction()
    store.update(tid, Record("A", "second version"))
    time.sleep(1)
    store.commit_transaction(tid)

def reader(store):
    tid = store.begin_transaction()
    results = {r.id: r.value for r in store.read(tid)}
    print("Reader sees:", results)
    store.commit_transaction(tid)

def main():
    store = Store()

    # Initial insert and commit
    txn1 = store.begin_transaction()
    store.insert(txn1, Record("A", "first version"))
    store.insert(txn1, Record("B", "first version"))
    store.commit_transaction(txn1)

    # Uncommitted updates
    txn2 = store.begin_transaction()
    store.update(txn2, Record("A", "second version"))
    store.update(txn2, Record("B", "second version"))
    print("User1 updated A and B (not committed).")

    # Concurrent read
    txn3 = store.begin_transaction()
    results = store.read(txn3)
    store.commit_transaction(txn3)

    print("User2 read during uncommitted updates:")
    for r in results:
        print(" ", r.id, "→", r.value)

    # Start updater thread
    t1 = threading.Thread(target=updater, args=(store,))
    t1.start()
    # Give updater a head start, then read
    time.sleep(0.2)
    reader(store)
    t1.join()
    # Read again after update
    reader(store)

if __name__ == "__main__":
    main()