"""
Test Scenario: Concurrent Inserts to Different Keys

Purpose:
Verify that MVCC allows concurrent inserts of different records
without conflict or data loss.

Idea:
- User1 inserts record "A" in txn1
- User2 inserts record "B" in txn2
- Both commit

Expected Result:
- A follow-up read should show both records "A" and "B"
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mvcc.store import Store
from mvcc.record import Record

def main():
    store = Store()

    txn1 = store.begin_transaction()
    store.insert(txn1, Record("A"))

    txn2 = store.begin_transaction()
    store.insert(txn2, Record("B"))

    store.commit_transaction(txn1)
    store.commit_transaction(txn2)

    txn3 = store.begin_transaction()
    results = store.read(txn3)
    store.commit_transaction(txn3)

    print("Records visible after both commits:")
    for r in results:
        print(" ", r.id)

if __name__ == "__main__":
    main()
