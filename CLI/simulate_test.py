import threading
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mvcc.store import Store
from mvcc.record import Record
import time

store = Store()

def user1():
    # User 1: Insert record A with value
    txn = store.begin_transaction()
    store.insert(txn, Record("A", "First value"))
    time.sleep(1)  # simulate slow work
    store.commit_transaction(txn)
    print("User1 committed insert A with value 'First value'")

    # User 1: Update record A
    txn = store.begin_transaction()
    store.update(txn, Record("A", "Updated value"))
    store.commit_transaction(txn)
    print("User1 committed update A with value 'Updated value'")

def user2():
    time.sleep(0.5)  # start slightly after user1
    txn = store.begin_transaction()
    results = store.read(txn)
    print("User2 read during User1 first txn:")
    for r in results:
        print(f"  {r.id} → {r.value}")
    store.commit_transaction(txn)

    time.sleep(1)  # wait for User1's update
    txn = store.begin_transaction()
    results = store.read(txn)
    print("User2 read after User1's update:")
    for r in results:
        print(f"  {r.id} → {r.value}")
    store.commit_transaction(txn)

def main():
    t1 = threading.Thread(target=user1)
    t2 = threading.Thread(target=user2)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

if __name__ == "__main__":
    main()
