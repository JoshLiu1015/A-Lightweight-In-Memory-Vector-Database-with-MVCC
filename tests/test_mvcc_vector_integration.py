from mvcc.cli_core import run_script
from vector_search.vector_store import get_all_vectors, reset_store
from mvcc.store import Store
from vector_search.utils import string_to_vector, get_top_k_keys

def test_mvcc_insert_triggers_vector_store():
    reset_store()
    # Insert via MVCC
    store = Store()
    script = """
        begin
        insert doc1 this is about cats
        insert doc2 this is about dogs
        commit
    """
    run_script(script, user="alice", store=store)
    # Now check vector store
    vectors = get_all_vectors()
    keys = [v["key"] for v in vectors]
    assert "doc1_1" in keys 
    assert "doc2_1" in keys

def test_mvcc_query_vector_search():
    reset_store()
    store = Store()
    script = """
        begin
        insert doc1 The quick brown fox jumps over the lazy dog
        insert doc2 Apple unveils new iPhone at tech event
        commit
        begin
        query Apple smartphone
        commit
    """
    out = run_script(script, user="alice", store=store)
    print("Query output:", out[-2])
    # Check that doc2 is in the query output
    assert "doc2" in out[-2]

def test_mvcc_update_triggers_vector_update():
    reset_store()
    store = Store()
    script = """
        begin
        insert doc1 original value
        commit
        begin
        update doc1 updated value
        commit
    """
    run_script(script, user="alice", store=store)
    # Query with a string similar to the new value
    query_vec = string_to_vector("updated value")
    results = get_top_k_keys(query_vec, get_all_vectors(), k=1)
    assert any(r["key"].startswith("doc1") for r in results)



def test_mvcc_concurrent_inserts_same_key():
    reset_store()
    store = Store()
    # Alice and Bob both try to insert doc1, only one should succeed
    alice_script = """
        begin
        insert doc1 alice's version
        commit
    """
    bob_script = """
        begin
        insert doc1 bob's version
    """
    run_script(alice_script, user="alice", store=store)
    run_script(bob_script, user="bob", store=store)
    vectors = get_all_vectors()
    # Only Alice's version should be present
    assert any(v["key"].startswith("doc1") for v in vectors)
    assert not any("bob" in v["key"] for v in vectors)

def test_mvcc_snapshot_isolation_vector_search():
    reset_store()
    store = Store()
    # Insert and commit
    run_script("begin\ninsert doc1 committed version\ncommit", user="alice", store=store)
    # Start a new transaction and update (uncommitted)
    shell = Store()
    script = """
        begin
        update doc1 uncommitted version
    """
    run_script(script, user="bob", store=store)
    # Query from a new transaction (should see only committed version)
    query_vec = string_to_vector("committed version")
    results = get_top_k_keys(query_vec, get_all_vectors(), k=1)
    assert any(r["key"].startswith("doc1") for r in results)

def test_mvcc_multiple_users_multiple_keys():
    reset_store()
    store = Store()
    run_script("begin\ninsert doc1 userA\ncommit", user="alice", store=store)
    run_script("begin\ninsert doc2 userB\ncommit", user="bob", store=store)
    vectors = get_all_vectors()
    keys = [v["key"] for v in vectors]
    assert any(k.startswith("doc1") for k in keys)
    assert any(k.startswith("doc2") for k in keys)

def test_mvcc_valid_keys_filter():
    reset_store()
    store = Store()
    run_script("begin\ninsert doc1 apple\ninsert doc2 banana\ninsert doc3 cherry\ncommit", user="alice", store=store)
    query_vec = string_to_vector("fruit")
    all_vectors = get_all_vectors()
    results = get_top_k_keys(query_vec, all_vectors, k=3, valid_keys=["doc1_1", "doc3_1"])
    keys = [r["key"] for r in results]
    assert all(k in ["doc1_1", "doc3_1"] for k in keys)

def test_mvcc_reinsert_after_delete():
    reset_store()
    store = Store()
    # Insert and commit
    run_script("begin\ninsert doc1 first\ncommit", user="alice", store=store)
    # Delete and commit
    run_script("begin\ndelete doc1\ncommit", user="alice", store=store)
    # Re-insert and commit
    run_script("begin\ninsert doc1 second\ncommit", user="alice", store=store)
    # Query with a string similar to the new value
    query_vec = string_to_vector("second")
    results = get_top_k_keys(query_vec, get_all_vectors(), k=1)
    assert any(r["key"].startswith("doc1") for r in results)

def test_mvcc_query_after_delete_same_txn():
    reset_store()
    store = Store()
    # Insert and commit
    run_script("begin\ninsert doc1 gone\ncommit", user="alice", store=store)
    # Begin new txn, delete, query before commit
    script = """
        begin
        delete doc1
        query
        commit
    """
    out = run_script(script, user="alice", store=store)
    # The deleted record should not be in the query output
    assert "doc1" not in out[-2]

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
    print("All integration tests passed!")
