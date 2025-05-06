import math

class Record:
    def __init__(self, id: str):
        self.id = id
        self.key = ""
        self.begin_ts: int | float = 0
        self.end_ts: int | float = math.inf
        self.deleted: bool = False
        self.created_by_txn_id: int | None = None
        self.next: Record | None = None