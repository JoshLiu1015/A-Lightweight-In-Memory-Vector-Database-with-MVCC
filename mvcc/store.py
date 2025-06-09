import sys
import os

# Insert the parent directory (project root) at the front of sys.path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

import threading
import time
import math
from .record import Record
from .transaction import Transaction, TransactionStatus
from vector_search import utils, vector_store

class Store:
    def __init__(self):
        self.records: dict[str, Record] = {}
        self.transactions: dict[int, Transaction] = {}
        self.lock = threading.RLock()
        self.current_txn_id = 0

    def begin_transaction(self) -> int:
        with self.lock:
            self.current_txn_id += 1
            txn = Transaction(self.current_txn_id)
            
            self.transactions[txn.id] = txn

            # Initialize snapshot data
            self.read(txn.id, "", 0)

            return txn.id

    def insert(self, txn_id: int, record: Record) -> None:
        record.begin_ts = txn_id
        record.end_ts = math.inf
        record.deleted = False
        record.created_by_txn_id = txn_id
        record.key = record.id + "_" + str(txn_id)

        with self.lock:
            head = self.records.get(record.id)
            if head is not None and not head.deleted:
                raise Exception(f"record with ID {record.id} already exists")
            self.records[record.id] = record

            # update the transaction's snapshot data
            txn = self.transactions[txn_id]
            if getattr(txn, "snapshot_data", None) is not None:
                # replace any older version of this key in the snapshot
                txn.snapshot_data = [
                    r for r in txn.snapshot_data if r.id != record.id
                ] + [record]

            vector = utils.string_to_vector(record.value)
            vector_store.add_vector(record.key, vector)

    def update(self, txn_id: int, record: Record) -> None:
        record.begin_ts = txn_id
        record.end_ts = math.inf
        record.deleted = False
        record.created_by_txn_id = txn_id
        record.key = record.id + "_" + str(txn_id)

        wasBlocked = False

        # Wait for existing head version to be committed or same txn
        while True:
            with self.lock:
                head = self.records.get(record.id)
            if head is None:
                raise Exception(f"record with ID {record.id} not found")
            creator_id = head.created_by_txn_id
            creator_txn = self.transactions.get(creator_id)
            if creator_id == txn_id or (creator_txn and creator_txn.status != TransactionStatus.ACTIVE):
                break
            
            if not wasBlocked:
                print("blocking by txn", creator_id, "for record", record.id)
                wasBlocked = True
            time.sleep(0.001)

        with self.lock:
            head = self.records[record.id]
            txn = self.transactions[txn_id]

            snapshot_version = next((r for r in txn.snapshot_data if r.id == record.id), None)
            if snapshot_version is not None and snapshot_version.key != head.key:
                raise Exception(f"Write conflict on record '{record.id}': ")
            
            
            record.next = head
            self.records[record.id] = record

            txn = self.transactions[txn_id]
            if getattr(txn, "snapshot_data", None) is not None:
                txn.snapshot_data = [
                    r for r in txn.snapshot_data if r.id != record.id
                ] + [record]

            vector = utils.string_to_vector(record.value)
            vector_store.add_vector(record.key, vector)


    def delete(self, txn_id: int, record_id: str) -> None:
        # treat delete as a new tombstone version
        with self.lock:
            head = self.records.get(record_id)
            if head is None:
                raise Exception(f"record with ID {record_id} not found")
            tombstone = Record(record_id, "")
            tombstone.begin_ts = txn_id
            tombstone.end_ts = math.inf
            tombstone.deleted = True
            tombstone.created_by_txn_id = txn_id
            tombstone.next = head
            self.records[record_id] = tombstone
            # update this txn's snapshot to hide deleted key
            txn = self.transactions[txn_id]
            if getattr(txn, "snapshot_data", None) is not None:
                txn.snapshot_data = [
                    r for r in txn.snapshot_data if r.id != record_id
                ]


    def read(self, txn_id: int, query: str, k: int) -> list[Record]:
        with self.lock:
            items = list(self.records.values())
            txns = dict(self.transactions)
            txn = txns.get(txn_id)

            if txn.snapshot_data != None:
                query_vector = utils.string_to_vector(query)
                return_keys = utils.get_top_k_keys(query_vector, [r.key for r in txn.snapshot_data], k=k)

                return_values = [s for s in txn.snapshot_data if s.key in return_keys]
                return return_values

        valid_records: list[Record] = []
        for head in items:
            current = head
            tombstone_by_this_txn = False

            while current:
                if current.created_by_txn_id == txn_id and current.deleted:
                    tombstone_by_this_txn = True
                    break

                creator_txn = txns.get(current.created_by_txn_id)
                if creator_txn and creator_txn.status == TransactionStatus.ACTIVE:
                    current = current.next
                    continue
                if current.begin_ts <= txn_id < current.end_ts and not current.deleted:
                    valid_records.append(current)
                    break
                current = current.next

            if tombstone_by_this_txn:
                continue
        txn.snapshot_data = valid_records

        query_vector = utils.string_to_vector(query)
        return_keys = utils.get_top_k_keys(query_vector, [r.key for r in valid_records], k=k)

        return_values = [s for s in valid_records if s.key in return_keys]
        return return_values

    def commit_transaction(self, txn_id: int) -> None:
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn:
                raise Exception(f"transaction {txn_id} not found")

            for head in self.records.values():
                current = head
                while current:
                    if current.created_by_txn_id == txn_id and current.next:
                        current.next.end_ts = current.begin_ts
                    current = current.next

            txn.status = TransactionStatus.COMMITTED

    def abort_transaction(self, txn_id: int) -> None:
        with self.lock:
            txn = self.transactions.get(txn_id)
            if not txn:
                raise Exception(f"transaction {txn_id} not found")
            # remove any versions created by this txn from the chains
            for key, head in list(self.records.items()):
                if head.created_by_txn_id == txn_id:
                    self.records[key] = head.next
                else:
                    prev = head
                    curr = head.next
                    while curr:
                        if curr.created_by_txn_id == txn_id:
                            prev.next = curr.next
                            break
                        prev, curr = curr, curr.next
            txn.status = TransactionStatus.ABORTED