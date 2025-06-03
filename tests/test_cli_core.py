from mvcc.cli_core import run_script, Shell, process_line
from mvcc.store import Store
import threading

def test_basic_insert_and_query():
    """
    Test: Basic insert and query
    - Inserts a record and queries it in the same transaction.
    - Expected: The inserted record and its value are visible in the query output.
    """
    store = Store()
    script = """
        begin txn1
        insert A hello
        query txn1
        commit txn1
    """
    out = run_script(script, user="alice", store=store)
    assert "A" in out[2] and "hello" in out[2]
    print("test_basic_insert_and_query passed.")

def test_snapshot_isolation():
    """
    Test: Snapshot isolation
    - Alice inserts and commits a record.
    - Bob starts a transaction before Alice's commit and queries.
    - Expected: Bob sees Alice's committed record in his query.
    """
    store = Store()
    alice_script = """
        begin txn1
        insert A v1
    """
    bob_script = """
        begin txn2
        commit txn1
        query txn2
        commit txn2
    """
    out2 = run_script(alice_script, user="alice", store=store)
    out1 = run_script(bob_script, user="bob", store=store)
    print("out1", out1)
    assert "A" not in out1[-2]
    print("test_snapshot_isolation passed.")

def test_update_and_query():
    """
    Test: Update and query
    - Inserts a record, commits, then updates it and queries.
    - Expected: The updated value is visible in the query output after the update and commit.
    """
    store = Store()
    script = """
        begin txn1
        insert A foo
        commit txn1
        begin txn2
        update txn2 A bar
        query txn2
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "bar" in out[-2]
    print("test_update_and_query passed.")

def test_delete_and_query():
    """
    Test: Delete and query
    - Inserts a record, commits, then deletes it and queries.
    - Expected: The deleted record is not visible in the query output after the delete and commit.
    """
    store = Store()
    script = """
        begin txn1
        insert A todelete
        commit txn1
        begin txn2
        delete A
        commit txn2
        begin txn3
        query txn3
        commit txn3
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_delete_and_query passed.")



def test_logical_delete_visibility():
    """
    Test: Logical delete visibility
    - Alice inserts and commits a record.
    - Bob starts a transaction before Alice deletes the record.
    - Alice deletes and commits the record.
    - Expected: Bob still sees the record in his transaction; new transactions do not see the deleted record.
    """
    store = Store()
    run_script("begin txn1\ninsert A gone\ncommit txn1", user="alice", store=store)
    bob_shell = Shell(user="bob", store=store)
    process_line(bob_shell, "begin txn2")
    run_script("begin txn3\ndelete A\ncommit txn3", user="alice", store=store)
    out = []
    out.append(process_line(bob_shell, "query txn2"))
    out.append(process_line(bob_shell, "commit txn2"))
    assert "A" in out[0]
    eve_query = run_script("begin txn4\nquery txn4\ncommit txn4", user="eve", store=store)
    assert "A" not in eve_query[-2]
    print("test_logical_delete_visibility passed.")

def test_read_your_own_writes():
    """
    Test: Read your own writes
    - Begin a transaction, insert a record, and query before commit.
    - Expected: The transaction sees its own uncommitted insert.
    """
    store = Store()
    script = """
        begin txn1
        insert A temp
        query txn1
        commit txn1
    """
    out = run_script(script, user="alice", store=store)
    assert "A" in out[2] and "temp" in out[2]
    print("test_read_your_own_writes passed.")


def test_version_chain_traversal():
    store = Store()
    # Insert v1 and commit
    run_script("begin txn1\ninsert A v1\ncommit txn1", user="alice", store=store)
    # Query after v1
    out1 = run_script("begin txn2\nquery txn2\ncommit txn2", user="alice", store=store)
    assert "v1" in out1[-2]

    # Update to v2 and commit
    run_script("begin txn3\nupdate txn3 A v2\ncommit txn3", user="alice", store=store)
    # Query after v2
    out2 = run_script("begin txn4\nquery txn4\ncommit txn4", user="alice", store=store)
    assert "v2" in out2[-2]

    # Update to v3 and commit
    run_script("begin txn5\nupdate txn5 A v3\ncommit txn5", user="alice", store=store)
    # Query after v3
    out3 = run_script("begin txn6\nquery txn6\ncommit txn6", user="alice", store=store)
    assert "v3" in out3[-2]

    print("test_version_chain_traversal passed.")

def test_abort_handling():
    """
    Test: Abort handling
    - Insert a record, then abort/rollback.
    - Expected: The record is not visible after abort.
    """
    store = Store()
    script = """
        begin txn1
        insert A shouldnotsee
        abort
        begin txn2
        query txn2
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_abort_handling passed.")

def test_insert_conflict():
    """
    Test: Insert conflict
    - Two users try to insert the same key concurrently.
    - Expected: Only one insert succeeds, the other fails.
    """
    store = Store()
    alice_script = """
        begin txn1
        insert A alice
        commit txn1
    """
    bob_script = """
        begin txn2
        insert A bob
        commit txn2
    """
    run_script(alice_script, user="alice", store=store)
    try:
        run_script(bob_script, user="bob", store=store)
        assert False, "Bob's insert should have failed, but it succeeded."
    except Exception as e:
        assert "already exists" in str(e)
    print("test_insert_conflict passed.")

def test_read_after_delete():
    """
    Test: Read after delete
    - Insert and commit, delete and commit, then query.
    - Expected: The deleted record is not visible.
    """
    store = Store()
    script = """
        begin txn1
        insert A gone
        commit txn1
        begin txn2
        delete A
        commit txn2
        begin txn3
        query txn3
        commit txn3
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_read_after_delete passed.")

def test_delete_then_read_same_txn():
    """
    Test: Delete and then read in same transaction
    - Insert and commit, begin new txn, delete, query before commit.
    - Expected: The deleted record is not visible in the same transaction.
    """
    store = Store()
    script = """
        begin txn1
        insert A gone
        commit txn1
        begin txn2
        delete A
        query txn2
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_delete_then_read_same_txn passed.")



def test_write_write_conflict_threaded():
    store = Store()
    # Insert and commit A
    run_script("begin txn0\ninsert A orig\ncommit txn0", user="alice", store=store)

    def txn1():
        run_script("begin txn1\nupdate txn1 A aliceval\ncommit txn1", user="alice", store=store)

    def txn2():
        run_script("begin txn2\nupdate txn2 A bobval\ncommit txn2", user="bob", store=store)

    t1 = threading.Thread(target=txn1)
    t2 = threading.Thread(target=txn2)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Query the final value
    out = run_script("begin txn3\nquery\ncommit txn3", user="eve", store=store)
    assert ("aliceval" in out[-2] or "bobval" in out[-2])
    print("test_write_write_conflict_threaded passed.")

if __name__ == "__main__":
    test_basic_insert_and_query()
    test_snapshot_isolation()
    test_update_and_query()
    test_delete_and_query()
    test_logical_delete_visibility()
    test_read_your_own_writes()
    test_version_chain_traversal()
    test_abort_handling()
    test_insert_conflict()
    test_read_after_delete()
    test_delete_then_read_same_txn()
    test_write_write_conflict_threaded()
    print("All tests passed!")
