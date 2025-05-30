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

def test_logical_delete_visibility():
    store = Store()
    # Insert and commit
    tid1 = store.begin_transaction()
    store.insert(tid1, Record("A", "delete me"))
    store.commit_transaction(tid1)
    # Delete and commit
    tid2 = store.begin_transaction()
    store.delete(tid2, "A")
    store.commit_transaction(tid2)
    # Try to read after delete
    tid3 = store.begin_transaction()
    results = {r.id: r.value for r in store.read(tid3)}
    print("After delete, records:", results)
    assert "A" not in results
    # Re-insert and commit
    tid4 = store.begin_transaction()
    store.insert(tid4, Record("A", "gone now"))
    store.commit_transaction(tid4)
    # Read again
    tid5 = store.begin_transaction()
    results2 = {r.id: r.value for r in store.read(tid5)}
    print("After re-insert, records:", results2)
    assert results2["A"] == "gone now"

if __name__ == "__main__":
    test_logical_delete_visibility()