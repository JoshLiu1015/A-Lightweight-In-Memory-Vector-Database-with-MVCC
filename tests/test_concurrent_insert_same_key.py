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
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from mvcc.store import Store
from mvcc.record import Record

def main():
    store = Store()

    txn1 = store.begin_transaction()
    txn2 = store.begin_transaction()

    try:
        store.insert(txn1, Record("A"))
        print("User1 inserted A.")
    except Exception as e:
        print("User1 insert failed:", e)

    try:
        store.insert(txn2, Record("A"))
        print("User2 inserted A.")
    except Exception as e:
        print("User2 insert failed:", e)

    store.commit_transaction(txn1)
    store.commit_transaction(txn2)

if __name__ == "__main__":
    main()
