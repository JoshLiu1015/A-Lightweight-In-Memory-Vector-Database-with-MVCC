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

from mvcc.store import Store
from mvcc.record import Record

def test_concurrent_insert_different_keys():
    store = Store()
    tid1 = store.begin_transaction()
    tid2 = store.begin_transaction()
    store.insert(tid1, Record("A", "how are you"))
    print("User1 inserted A.")
    store.insert(tid2, Record("B", "i am fine"))
    print("User2 inserted B.")
    store.commit_transaction(tid1)
    store.commit_transaction(tid2)

    tid3 = store.begin_transaction()
    results = store.read(tid3)
    store.commit_transaction(tid3)
    print("Records visible after both commits:")
    for r in results:
        print(" ", r.id, "→", r.value)

if __name__ == "__main__":
    test_concurrent_insert_different_keys()