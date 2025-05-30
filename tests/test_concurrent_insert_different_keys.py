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
import threading
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mvcc.store import Store
from mvcc.record import Record

def user1(store):
    tid = store.begin_transaction()
    try:
        store.insert(tid, Record("A", "how are you"))
        print("User1 inserted A.")
    except Exception as e:
        print("User1 insert failed:", e)
    store.commit_transaction(tid)

def user2(store):
    tid = store.begin_transaction()
    try:
        store.insert(tid, Record("B", "i am fine"))
        print("User2 inserted B.")
    except Exception as e:
        print("User2 insert failed:", e)
    store.commit_transaction(tid)

if __name__ == "__main__":
    store = Store()
    t1 = threading.Thread(target=user1, args=(store,))
    t2 = threading.Thread(target=user2, args=(store,))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    txn3 = store.begin_transaction()
    results = store.read(txn3)
    store.commit_transaction(txn3)

    print("Records visible after both commits:")
    for r in results:
        print(" ", r.id)