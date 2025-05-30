import threading
import time
import math
from record import Record
from transaction import Transaction, TransactionStatus
from vector_utils import convert_text_to_vector, send_vector
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
            self.read(txn.id)

            return txn.id

    def insert(self, txn_id: int, record: Record) -> None:
        record.begin_ts = txn_id
        record.end_ts = math.inf
        record.deleted = False
        record.created_by_txn_id = txn_id
        record.key = record.id + "_" + str(txn_id)

        with self.lock:
            if record.id in self.records:
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
            time.sleep(0.001)

        with self.lock:
            head = self.records[record.id]
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
        with self.lock:
            head = self.records.get(record_id)
            if head is None:
                raise Exception(f"record with ID {record_id} not found")
            head.deleted = True
            head.end_ts = txn_id
            self.records[record_id] = head

    def read(self, txn_id: int) -> list[Record]:
        with self.lock:
            items = list(self.records.values())
            txns = dict(self.transactions)
            txn = txns.get(txn_id)

            if txn.snapshot_data:
                return list(txn.snapshot_data)

        valid_records: list[Record] = []
        for head in items:
            current = head
            while current:
                creator_txn = txns.get(current.created_by_txn_id)
                if creator_txn and creator_txn.status == TransactionStatus.ACTIVE:
                    current = current.next
                    continue
                if current.begin_ts <= txn_id < current.end_ts and not current.deleted:
                    valid_records.append(current)
                    break
                current = current.next

        txn.snapshot_data = valid_records
        return valid_records

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
