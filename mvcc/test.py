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
        rec_a_v1 = Record("A", "mock data for A")
        store.insert(tx1, rec_a_v1)
        # key should be "<id>_<txn>"
        self.assertEqual(rec_a_v1.key, "A_1")
        store.commit_transaction(tx1)

        # ─── Transaction 2: Insert B and commit ───────────────────────
        tx2 = store.begin_transaction()
        rec_b_v1 = Record("B", "mock data for B")
        store.insert(tx2, rec_b_v1)
        self.assertEqual(rec_b_v1.key, "B_2")
        store.commit_transaction(tx2)

        # ─── Transaction 3: Update A (but do NOT commit) ─────────────
        tx3 = store.begin_transaction()
        rec_a_v2 = Record("A", "updated mock data for A")
        store.update(tx3, rec_a_v2)
        self.assertEqual(rec_a_v2.key, "A_3")
        # (no commit here)

        # ─── Transaction 4: Read snapshot — should see only v1 keys ───
        tx4 = store.begin_transaction()
        visible_keys = { r.id: r.key for r in store.read(tx4) }
        # A#1 still visible, B#2 visible, but A#3 is uncommitted so skipped
        self.assertDictEqual(visible_keys, {"A": "A_1", "B": "B_2"})

        # ─── Now commit the update ─────────────────────────────────────
        store.commit_transaction(tx3)

        # ─── Transaction 5: Read again — should see the new A#3 ───────
        tx5 = store.begin_transaction()
        visible_keys2 = { r.id: r.key for r in store.read(tx5) }
        self.assertDictEqual(visible_keys2, {"A": "A_3", "B": "B_2"})

    
    def test2_mvcc_snapshot_isolation_with_keys(self):
        store = Store()

        # ─── Transaction 1: Insert A and commit ───────────────────────
        tx1 = store.begin_transaction()
        rec_a_v1 = Record("A", "mock data for A")
        store.insert(tx1, rec_a_v1)
        # key should be "<id>_<txn>"
        self.assertEqual(rec_a_v1.key, "A_1")
        store.commit_transaction(tx1)

        # ─── Transaction 2: Insert B and commit ───────────────────────
        tx2 = store.begin_transaction()
        rec_b_v1 = Record("B", "mock data for B")
        store.insert(tx2, rec_b_v1)
        self.assertEqual(rec_b_v1.key, "B_2")
        store.commit_transaction(tx2)

        # ─── Transaction 3: Update A (but do NOT commit) ─────────────
        tx3 = store.begin_transaction()
        rec_a_v2 = Record("A", "updated mock data for A")
        store.update(tx3, rec_a_v2)
        self.assertEqual(rec_a_v2.key, "A_3")
        # (no commit here)

        # ─── Transaction 4: Read snapshot — should see only v1 keys ───
        tx4 = store.begin_transaction()
        visible_keys = { r.id: r.key for r in store.read(tx4) }
        # A#1 still visible, B#2 visible, but A#3 is uncommitted so skipped
        self.assertDictEqual(visible_keys, {"A": "A_1", "B": "B_2"})

        # ─── Now commit the update ─────────────────────────────────────
        store.commit_transaction(tx3)

        # ─── Transaction 4: Read again — should still see only v1 keys ───────
        visible_keys = { r.id: r.key for r in store.read(tx4) }
        # A#1 still visible, B#2 visible, but A#3 is uncommitted so skipped
        self.assertDictEqual(visible_keys, {"A": "A_1", "B": "B_2"})

if __name__ == "__main__":
    unittest.main()
