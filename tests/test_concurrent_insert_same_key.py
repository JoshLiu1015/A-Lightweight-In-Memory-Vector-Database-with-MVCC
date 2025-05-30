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

import sys
import os
import threading
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mvcc.store import Store
from mvcc.record import Record

def user1(store):
    tid = store.begin_transaction()
    try:
        store.insert(tid, Record("A", "hehehe"))
        print("User1 inserted A.")
    except Exception as e:
        print("User1 insert failed:", e)
    store.commit_transaction(tid)

def user2(store):
    tid = store.begin_transaction()
    try:
        store.insert(tid, Record("A", "i look good"))
        print("User2 inserted A.")
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