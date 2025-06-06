from .record import Record

class TransactionStatus:
    ACTIVE = "Active"
    COMMITTED = "Committed"
    ABORTED = "Aborted"


class Transaction:
    def __init__(self, txn_id: int):
        self.id = txn_id
        self.status = TransactionStatus.ACTIVE
        self.start_ts = txn_id  # simplified timestamp
        self.snapshot_data: list[Record] = None