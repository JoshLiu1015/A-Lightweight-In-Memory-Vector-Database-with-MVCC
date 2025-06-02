# tests/test_mvcc_store_keys.py

import unittest
import math
from record import Record
from store import Store

class TestSimpleMVCCKeys(unittest.TestCase):
    def test1_mvcc_snapshot_isolation_with_keys(self):
        store = Store()

        # ─── Transaction 1: Insert A and commit ───────────────────────
        tx1 = store.begin_transaction()
        rec_a_v1 = Record("A", "sports: NBA playoffs continue into finals weekend")
        store.insert(tx1, rec_a_v1)
        # key should be "<id>_<txn>"
        self.assertEqual(rec_a_v1.key, "A_1")
        store.commit_transaction(tx1)

        # ─── Transaction 2: Insert B and commit ───────────────────────
        tx2 = store.begin_transaction()
        rec_b_v1 = Record("B", "tech: Apple unveils its latest AR headset")
        store.insert(tx2, rec_b_v1)
        self.assertEqual(rec_b_v1.key, "B_2")
        store.commit_transaction(tx2)

        # ─── Transaction 3: Update A (but do NOT commit) ─────────────
        tx3 = store.begin_transaction()
        rec_a_v2 = Record("A", "sports: new update on NBA playoffs")
        store.update(tx3, rec_a_v2)
        self.assertEqual(rec_a_v2.key, "A_3")
        # (no commit here)

        # ─── Transaction 4: Read snapshot — should see only v1 keys ───
        tx4 = store.begin_transaction()
        visible_keys_values = { r.id: r.key + ": " + r.value for r in store.read(tx4, "sport", 1) }
        print("Visible keys and values in tx4:", visible_keys_values)
        # A#1 still visible, B#2 visible, but A#3 is uncommitted so skipped
        self.assertDictEqual(visible_keys_values, {"A": "A_1: sports: NBA playoffs continue into finals weekend"})

        # ─── Now commit the update ─────────────────────────────────────
        store.commit_transaction(tx3)

        # ─── Transaction 5: Read again — should see the new A#3 ───────
        tx5 = store.begin_transaction()
        visible_keys_values = { r.id: r.key + ": " + r.value for r in store.read(tx5, "sport", 1) }
        print("Visible keys and values in tx5:", visible_keys_values)
        self.assertDictEqual(visible_keys_values, {"A": "A_3: sports: new update on NBA playoffs"})

        
if __name__ == "__main__":
    unittest.main()
