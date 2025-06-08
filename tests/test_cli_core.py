from CLI.cli_core import run_script
from mvcc.store import Store
import threading

def test_basic_insert_and_query():
    """
    Test: Basic insert and query
    - Inserts a record and queries it in the same transaction.
    - Expected: The inserted record and its value are visible in the query output.
    """

    print("Starting test_basic_insert_and_query...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A hello
        query txn1
        commit txn1
    """
    out = run_script(script, user="alice", store=store)
    assert "A" in out[2] and "hello" in out[2]
    print("test_basic_insert_and_query passed.\n\n")

def test_snapshot_isolation():
    """
    Test: Snapshot isolation
    - txn1 inserts and commits a record.
    - txn2 starts a transaction before txn1 commit and queries.
    - Expected: txn2 won't see txn1 committed record in txn2's query.
    """

    print("Starting test_snapshot_isolation...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A v1
        begin txn2
        commit txn1
        query txn2
        commit txn2
    """
    
    output = run_script(script, store=store)
    assert "A" not in output[4]
    print("test_snapshot_isolation passed.\n\n")

def test_update_and_query():
    """
    Test: Update and query
    - Inserts a record, commits, then updates it and queries.
    - Expected: The updated value is visible in the query output after the update and commit.
    """

    print("Starting test_update_and_query...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A foo
        commit txn1
        begin txn2
        update txn2 A bar
        query txn2
        commit txn2
    """
    out = run_script(script, store=store)
    assert "bar" in out[-2]
    print("test_update_and_query passed.\n\n")

def test_delete_and_query():
    """
    Test: Delete and query
    - Inserts a record, commits, then deletes it and queries.
    - Expected: The deleted record is not visible in the query output after the delete and commit.
    """

    print("Starting test_delete_and_query...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A todelete
        commit txn1
        begin txn2
        delete txn2 A
        commit txn2
        begin txn3
        query txn3
        commit txn3
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_delete_and_query passed.\n\n")



def test_read_your_own_writes():
    """
    Test: Read your own writes
    - Within a single transaction, a write is made and then read back before committing.
    - The transaction should see its own uncommitted changes.
    """

    print("Starting test_read_your_own_writes...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A gone
        commit txn1
        begin txn2
        delete txn2 A
        query txn2
        commit txn2
    """
    out = run_script(script, store=store)
    assert "A" not in out[-2]
    print("test_read_your_own_writes passed.\n\n")

def test_version_chain_traversal():
    """
    Add multiple versions of a record (R1, R2, R3 by different transactions).
    A read should select the correct version based on its snapshot timestamp.
    """

    print("Starting test_version_chain_traversal...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A v1
        commit txn1
        begin txn2
        query tnx2
        commit txn2
        begin txn3
        update txn3 A v2
        commit txn3
        begin txn4
        update txn4 A v3
        query txn4
        commit txn4
    """
    out = run_script(script, store=store)
    assert "v1" in out[4] and "v3" in out[-2]

    print("test_version_chain_traversal passed.\n\n")

def test_abort_handling():
    """
    Test: Abort handling
    - Insert a record, then abort/rollback.
    - Expected: The record is not visible after abort.
    """

    print("Starting test_abort_handling...")
    store = Store()
    script = """
        begin txn1
        insert txn1 A shouldnotsee
        abort txn1
        begin txn2
        query txn2
        commit txn2
    """
    out = run_script(script, user="alice", store=store)

    assert "A" not in out[-2]
    print("test_abort_handling passed.\n\n")

def test_insert_conflict():
    """
    Test: Insert conflict
    - Two users try to insert the same key concurrently.
    - Expected: Only one insert succeeds, the other fails.
    """

    print("Starting test_insert_conflict...")
    store = Store()
    alice_script = """
        begin txn1
        insert txn1 A alice
        commit txn1
    """
    bob_script = """
        begin txn2
        insert txn2 A bob
        commit txn2
    """
    run_script(alice_script, user="alice", store=store)
    out = run_script(bob_script, user="bob", store=store)
    assert "Write conflict on record 'A'" in out[-2] or "already exists" in out[-2]

    print("test_insert_conflict passed.\n\n")



def test_write_write_conflict_threaded():
    """
    Tests that a concurrent update to the same record causes a write conflict. 
    txn2 updates record A and 
    txn3 tries to update A while txn2 is uncommitted, 
    blocks, then detects the conflict and is aborted. Validates correct conflict detection under MVCC.
    """

    print("Starting test_write_write_conflict_threaded...")
    store = Store()
    # Insert and commit A

    def txn1_and_2():
        script1 = """
            begin txn1
            insert txn1 A orig
            commit txn1
            begin txn2
            update txn2 A aliceval
            sleep
            commit txn2
        """

        run_script(script1, store=store)
    
    
    def txn3():
        import time
        time.sleep(3)

        out = run_script("begin txn3\nupdate txn3 A bobval\ncommit txn3", store=store)
        assert "Write conflict on record 'A'" in out[-2]



    t1 = threading.Thread(target=txn1_and_2)
    t2 = threading.Thread(target=txn3)

    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    print("test_write_write_conflict_threaded passed.\n\n")
 

if __name__ == "__main__":
    test_basic_insert_and_query()
    test_snapshot_isolation()
    test_update_and_query()
    test_delete_and_query()
    test_read_your_own_writes()
    test_version_chain_traversal()
    test_abort_handling()
    test_insert_conflict()
    test_write_write_conflict_threaded()
    print("All tests passed!")
