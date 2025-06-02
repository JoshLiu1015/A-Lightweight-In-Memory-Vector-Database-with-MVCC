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
from mvcc.store import Store
from mvcc.record import Record

def test_query_during_update():
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

    # Concurrent read (should not see uncommitted updates)
    txn3 = store.begin_transaction()
    results = store.read(txn3)
    store.commit_transaction(txn3)
    print("User2 read during uncommitted updates:")
    for r in results:
        print(" ", r.id, "→", r.value)

    # Now commit the updates
    store.commit_transaction(txn2)

    # Read again (should see updated values)
    txn4 = store.begin_transaction()
    results2 = store.read(txn4)
    store.commit_transaction(txn4)
    print("User2 read after commit:")
    for r in results2:
        print(" ", r.id, "→", r.value)

if __name__ == "__main__":
    test_query_during_update()