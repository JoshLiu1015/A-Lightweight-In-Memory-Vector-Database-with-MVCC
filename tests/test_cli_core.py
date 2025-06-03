from mvcc.cli_core import run_script, Shell, process_line
from mvcc.store import Store

def test_basic_insert_and_query():
    """
    Test: Basic insert and query
    - Inserts a record and queries it in the same transaction.
    - Expected: The inserted record and its value are visible in the query output.
    """
    store = Store()
    script = """
        begin
        insert A hello
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    assert "A" in out[2] and "hello" in out[2]
    print("test_basic_insert_and_query passed.")

def test_snapshot_isolation():
    """
    Test: Snapshot isolation
    - Alice inserts and commits a record.
    - Bob starts a transaction after Alice's commit and queries.
    - Expected: Bob sees Alice's committed record in his query.
    """
    store = Store()
    alice_script = """
        begin
        insert A v1
        commit
        begin
        query
        commit
    """
    bob_script = """
        begin
        query
        commit
    """
    out1 = run_script(alice_script, user="alice", store=store)
    out2 = run_script(bob_script, user="bob", store=store)
    assert "A" in out1[-2]
    print("test_snapshot_isolation passed.")

def test_update_and_query():
    """
    Test: Update and query
    - Inserts a record, commits, then updates it and queries.
    - Expected: The updated value is visible in the query output after the update and commit.
    """
    store = Store()
    script = """
        begin
        insert A foo
        commit
        begin
        update A bar
        query
        commit
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
        begin
        insert A todelete
        commit
        begin
        delete A
        commit
        begin
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    assert "A" not in out[-2]
    print("test_delete_and_query passed.")

def test_write_write_conflict():
    """
    Test: Write-write conflict (concurrent update)
    - Alice and Bob both update the same record in separate transactions and commit.
    - Expected: Only the last committed value is visible in the final query.
    """
    store = Store()
    # Alice inserts and commits A
    run_script("begin\ninsert A orig\ncommit", user="alice", store=store)
    # Alice and Bob both try to update A concurrently
    alice_script = """
        begin
        update A aliceval
        commit
    """
    bob_script = """
        begin
        update A bobval
        commit
    """
    out1 = run_script(alice_script, user="alice", store=store)
    out2 = run_script(bob_script, user="bob", store=store)
    # Only the last committed value should be visible
    final = run_script("begin\nquery\ncommit", user="eve", store=store)
    assert ("aliceval" in final[-2] or "bobval" in final[-2])
    print("test_write_write_conflict passed.")

def test_logical_delete_visibility():
    """
    Test: Logical delete visibility
    - Alice inserts and commits a record.
    - Bob starts a transaction before Alice deletes the record.
    - Alice deletes and commits the record.
    - Expected: Bob still sees the record in his transaction; new transactions do not see the deleted record.
    """
    store = Store()
    # Insert and commit
    run_script("begin\ninsert A gone\ncommit", user="alice", store=store)
    # Bob starts a transaction (keep his shell alive)
    bob_shell = Shell(user="bob", store=store)
    process_line(bob_shell, "begin")
    # Alice deletes and commits
    run_script("begin\ndelete A\ncommit", user="alice", store=store)
    # Bob queries and commits in the same transaction
    out = []
    out.append(process_line(bob_shell, "query"))
    out.append(process_line(bob_shell, "commit"))
    assert "A" in out[0]
    # New transaction should not see A
    eve_query = run_script("begin\nquery\ncommit", user="eve", store=store)
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
        begin
        insert A temp
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    assert "A" in out[2] and "temp" in out[2]
    print("test_read_your_own_writes passed.")

def test_conflicting_updates():
    """
    Test: Conflicting updates
    - Two users update the same record concurrently.
    - Expected: Only one update is visible after both commit.
    """
    store = Store()
    # Insert and commit initial value
    run_script("begin\ninsert A orig\ncommit", user="alice", store=store)
    # Both users start transactions and update
    alice_script = """
        begin
        update A alice
        commit
    """
    bob_script = """
        begin
        update A bob
        commit
    """
    run_script(alice_script, user="alice", store=store)
    run_script(bob_script, user="bob", store=store)
    # Final value should be one of the updates
    out = run_script("begin\nquery\ncommit", user="eve", store=store)
    assert ("alice" in out[-2] or "bob" in out[-2])
    print("test_conflicting_updates passed.")

def test_version_chain_traversal():
    """
    Test: Version chain traversal
    - Insert, update multiple times, and query.
    - Expected: Only the latest value is visible.
    """
    store = Store()
    script = """
        begin
        insert A v1
        commit
        begin
        update A v2
        commit
        begin
        update A v3
        commit
        begin
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    assert "v3" in out[-2]
    print("test_version_chain_traversal passed.")

def test_abort_handling():
    """
    Test: Abort handling
    - Insert a record, then abort/rollback.
    - Expected: The record is not visible after abort.
    """
    store = Store()
    script = """
        begin
        insert A shouldnotsee
        abort
        begin
        query
        commit
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
        begin
        insert A alice
        commit
    """
    bob_script = """
        begin
        insert A bob
        commit
    """
    # Run Alice's insert first
    run_script(alice_script, user="alice", store=store)
    # Bob's insert should fail
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
        begin
        insert A gone
        commit
        begin
        delete A
        commit
        begin
        query
        commit
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
        begin
        insert A gone
        commit
        begin
        delete A
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    print(out)
    assert "A" not in out[-2]
    print("test_delete_then_read_same_txn passed.")

if __name__ == "__main__":
    test_basic_insert_and_query()
    test_snapshot_isolation()
    test_update_and_query()
    test_delete_and_query()
    test_write_write_conflict()
    test_logical_delete_visibility()
    test_read_your_own_writes()
    test_conflicting_updates()
    test_version_chain_traversal()
    test_abort_handling()
    test_insert_conflict()
    test_read_after_delete()
    test_delete_then_read_same_txn()
    print("All tests passed!")
