from mvcc.cli_core import run_script
from vector_search.vector_store import reset_store
from mvcc.store import Store

def test_mvcc_insert_triggers_vector_store():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert doc1 this is about cats
        insert doc2 this is about dogs
        commit txn1
    """
    run_script(script, user="alice", store=store)
    # Query to check both docs are present
    script2 = """
        begin txn2
        query txn2 cats
        query txn2 dogs
        commit txn2
    """
    out = run_script(script2, user="alice", store=store)
    assert "doc1" in out[1]  # cats
    assert "doc2" in out[2]  # dogs

def test_mvcc_query_vector_search():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert doc1 The quick brown fox jumps over the lazy dog
        insert doc2 Apple unveils new iPhone at tech event
        commit txn1
        begin txn2
        query txn2 Apple smartphone
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    print("Query output:", out[-2])
    assert "doc2" in out[-2]

def test_mvcc_update_triggers_vector_update():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert doc1 original value
        commit txn1
        begin txn2
        update txn2 doc1 updated value
        commit txn2
        begin txn3
        query txn3 updated value
        commit txn3
    """
    out = run_script(script, user="alice", store=store)
    print("Query output after update:", out[-2])
    assert "doc1" in out[-2] and "updated value" in out[-2]

def test_mvcc_concurrent_inserts_same_key():
    reset_store()
    store = Store()
    alice_script = """
        begin txn1
        insert doc1 alice's version
        commit txn1
    """
    bob_script = """
        begin txn2
        insert doc1 bob's version
        query txn2 bob's version
        commit txn2
    """
    run_script(alice_script, user="alice", store=store)
    try:
        run_script(bob_script, user="bob", store=store)
        assert False, "Expected exception for duplicate insert, but none was raised."
    except Exception as e:
        assert "already exists" in str(e)

def test_mvcc_snapshot_isolation_vector_search():
    reset_store()
    store = Store()
    # Insert and commit
    run_script("begin txn1\ninsert doc1 committed version\ncommit txn1", user="alice", store=store)
    # Start a new transaction and update (uncommitted)
    run_script("begin txn2\nupdate txn2 doc1 uncommitted version", user="bob", store=store)
    # Query from a new transaction (should see only committed version)
    script = """
        begin txn3
        query txn3 committed version
        commit txn3
    """
    out = run_script(script, user="eve", store=store)
    assert "committed version" in out[1]

def test_mvcc_multiple_users_multiple_keys():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert doc1 userA\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\ninsert doc2 userB\ncommit txn2", user="bob", store=store)
    script = """
        begin txn3
        query txn3 userA
        query txn3 userB
        commit txn3
    """
    out = run_script(script, user="eve", store=store)
    assert "doc1" in out[1]
    assert "doc2" in out[2]

def test_mvcc_valid_keys_filter():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert doc1 apple\ninsert doc2 banana\ninsert doc3 cherry\ncommit txn1", user="alice", store=store)
    script = """
        begin txn2
        query txn2 apple
        query txn2 cherry
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1]
    assert "doc3" in out[2]

def test_mvcc_reinsert_after_delete():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert doc1 first\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\ndelete doc1\ncommit txn2", user="alice", store=store)
    run_script("begin txn3\ninsert doc1 second\ncommit txn3", user="alice", store=store)
    script = """
        begin txn4
        query txn4 second
        commit txn4
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1] and "second" in out[1]

def test_mvcc_query_after_delete_same_txn():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert doc1 gone\ncommit txn1", user="alice", store=store)
    script = """
        begin txn2
        delete doc1
        query txn2 gone
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" not in out[2]

def test_mvcc_vector_snapshot_isolation():
    reset_store()
    store = Store()
    # Insert and commit doc1
    run_script("begin txn1\ninsert doc1 version1\ncommit txn1", user="alice", store=store)
    # Begin txn2 before update
    run_script("begin txn2", user="bob", store=store)
    # Update doc1 in txn3 and commit
    run_script("begin txn3\nupdate txn3 doc1 version2\ncommit txn3", user="alice", store=store)
    # Query in txn2 (should see version1)
    out = run_script("query txn2 version1", user="bob", store=store)
    assert "version1" in out[0]
    # Query in a new txn4 (should see version2)
    out2 = run_script("begin txn4\nquery txn4 version2\ncommit txn4", user="eve", store=store)
    assert "version2" in out2[1]
    print("test_mvcc_vector_snapshot_isolation passed.")

def test_mvcc_valid_keys_after_update_delete():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert doc1 apple\ninsert doc2 banana\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\nupdate txn2 doc1 orange\ncommit txn2", user="alice", store=store)
    run_script("begin txn3\ndelete doc2\ncommit txn3", user="alice", store=store)
    script = """
        begin txn4
        query txn4 orange
        commit txn4
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1] and "orange" in out[1]
    print("test_mvcc_valid_keys_after_update_delete passed.")

if __name__ == "__main__":
    test_mvcc_insert_triggers_vector_store()
    test_mvcc_query_vector_search()
    test_mvcc_update_triggers_vector_update()
    test_mvcc_concurrent_inserts_same_key()
    test_mvcc_snapshot_isolation_vector_search()
    test_mvcc_multiple_users_multiple_keys()
    test_mvcc_valid_keys_filter()
    test_mvcc_reinsert_after_delete()
    test_mvcc_query_after_delete_same_txn()
    test_mvcc_vector_snapshot_isolation()
    test_mvcc_valid_keys_after_update_delete()
    print("All integration tests passed!")
