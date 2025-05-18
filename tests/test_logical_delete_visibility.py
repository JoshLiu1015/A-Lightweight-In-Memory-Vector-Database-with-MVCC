"""
FLAGGED

Test Scenario: Logical Delete Followed by Read

Purpose:
Ensure that a transaction that starts before a deletion still sees the record.

Idea:
- User1 inserts and commits record "A"
- User2 begins a read transaction
- User1 deletes A and commits
- User2 should still see A because it started before the delete

Expected Result:
- User2's read sees record "A"
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
    store.commit_transaction(txn1)

    txn2 = store.begin_transaction()
    # Start reading before deletion
    txn3 = store.begin_transaction()
    store.delete(txn3, "A")
    store.commit_transaction(txn3)

    results = store.read(txn2)
    store.commit_transaction(txn2)

    print("User2 read after deletion but started before:")
    for r in results:
        print(" ", r.id)

if __name__ == "__main__":
    main()
