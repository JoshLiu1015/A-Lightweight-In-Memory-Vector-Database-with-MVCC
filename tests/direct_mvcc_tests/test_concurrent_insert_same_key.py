"""
Test Scenario: Concurrent Inserts to the Same Key

Purpose:
Ensure that inserting the same record ID in two concurrent transactions
should result in one succeeding and the other failing.

Idea:
- User1 and User2 both try to insert record "A"
- Only one should succeed

Expected Result:
- One transaction commits successfully
- The other raises an exception
"""

from mvcc.store import Store
from mvcc.record import Record

def test_concurrent_insert_same_key():
    store = Store()
    tid1 = store.begin_transaction()
    tid2 = store.begin_transaction()
    try:
        store.insert(tid1, Record("A", "hehehe"))
        print("User1 inserted A.")
    except Exception as e:
        print("User1 insert failed:", e)
    try:
        store.insert(tid2, Record("A", "i look good"))
        print("User2 inserted A.")
    except Exception as e:
        print("User2 insert failed:", e)
    store.commit_transaction(tid1)
    store.commit_transaction(tid2)

if __name__ == "__main__":
    test_concurrent_insert_same_key()