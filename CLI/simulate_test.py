import threading
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mvcc.store import Store
from mvcc.record import Record
import time

store = Store()

def user1():
    txn = store.begin_transaction()
    store.insert(txn, Record("A"))
    time.sleep(1)  # simulate slow work
    store.commit_transaction(txn)
    print("User1 committed insert A")

def user2():
    time.sleep(0.5)  # start slightly after user1
    txn = store.begin_transaction()
    results = store.read(txn)
    store.commit_transaction(txn)
    print("User2 read during User1 txn:")
    for r in results:
        print("  ", r.id)

t1 = threading.Thread(target=user1)
t2 = threading.Thread(target=user2)

t1.start()
t2.start()

t1.join()
t2.join()
