"""
FLAGGED

Test Scenario: Logical Delete Followed by Read

Purpose:
Ensure that a transaction that starts before a deletion still sees the record.

Idea:
- User1 inserts and commits record "A"
- User2 begins a read transaction BEFORE delete
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
    # User1 inserts and commits record "A"
    tid1 = store.begin_transaction()
    store.insert(tid1, Record("A", "delete me"))
    store.commit_transaction(tid1)
    # User2 begins a read transaction BEFORE delete
    tid2 = store.begin_transaction()
    # User1 deletes A and commits
    tid3 = store.begin_transaction()
    store.delete(tid3, "A")
    store.commit_transaction(tid3)
    # User2 should still see A because it started before the delete
    results = {r.id: r.value for r in store.read(tid2)}
    print("User2 read after delete committed (should still see A):", results)
    assert "A" in results
    # User2 commits
    store.commit_transaction(tid2)
    # New transaction should not see A
    tid4 = store.begin_transaction()
    results2 = {r.id: r.value for r in store.read(tid4)}
    print("New transaction after delete (should NOT see A):", results2)
    assert "A" not in results2
    # Re-insert and commit
    tid5 = store.begin_transaction()
    store.insert(tid5, Record("A", "gone now"))
    store.commit_transaction(tid5)
    # Read again
    tid6 = store.begin_transaction()
    results3 = {r.id: r.value for r in store.read(tid6)}
    print("After re-insert, records:", results3)
    assert results3["A"] == "gone now"

if __name__ == "__main__":
    test_logical_delete_visibility()