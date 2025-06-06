from CLI.cli_core import run_script
from vector_search.vector_store import reset_store
from mvcc.store import Store
import ast

# Test that inserting records in a transaction triggers vector store updates and that both records are queryable by their content.
def test_mvcc_insert_triggers_vector_store():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert txn1 doc1 this is about cats
        insert txn1 doc2 this is about dogs
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

# Test that a query returns the correct document based on vector similarity after insertion and commit.
def test_mvcc_query_vector_search():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert txn1 doc1 The quick brown fox jumps over the lazy dog
        insert txn1 doc2 Apple unveils new iPhone at tech event
        commit txn1
        begin txn2
        query txn2 Apple smartphone
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc2" in out[-2]

# Test that updating a record in a new transaction updates the vector store, and the updated value is returned by a query.
def test_mvcc_update_triggers_vector_update():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert txn1 doc1 original value
        commit txn1
        begin txn2
        update txn2 doc1 updated value
        commit txn2
        begin txn3
        query txn3 updated value
        commit txn3
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[-2] and "updated value" in out[-2]

# Test that concurrent inserts of the same key are not allowed (write-write conflict is detected and prevented).
def test_mvcc_concurrent_inserts_same_key():
    reset_store()
    store = Store()
    alice_script = """
        begin txn1
        insert txn1 doc1 alice's version
        commit txn1
        begin txn2
        insert txn2 doc1 bob's version
        query txn2 bob's version
        commit txn2
    """
    try:
        run_script(alice_script, user="alice", store=store)
        assert False, "Expected exception for duplicate insert, but none was raised."
    except Exception as e:
        assert "already exists" in str(e)

# Test snapshot isolation: a transaction sees only committed versions, not uncommitted updates from other transactions.
def test_mvcc_snapshot_isolation_vector_search():
    reset_store()
    store = Store()
    # Insert and commit
    run_script("begin txn1\ninsert txn1 doc1 committed version\ncommit txn1", user="alice", store=store)
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

# Test that multiple users can insert different keys and both are queryable in a new transaction.
def test_mvcc_multiple_users_multiple_keys():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert txn1 doc1 userA\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\ninsert txn2 doc2 userB\ncommit txn2", user="bob", store=store)
    script = """
        begin txn3
        query txn3 userA
        query txn3 userB
        commit txn3
    """
    out = run_script(script, user="eve", store=store)
    assert "doc1" in out[1]
    assert "doc2" in out[2]

# Test that queries return only the correct keys based on content, simulating a valid_keys filter via content.
def test_mvcc_valid_keys_filter():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert txn1 doc1 apple\ninsert txn1 doc2 banana\ninsert txn1 doc3 cherry\ncommit txn1", user="alice", store=store)
    script = """
        begin txn2
        query txn2 apple
        query txn2 cherry
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1]
    assert "doc3" in out[2]

# Test that after deleting a key, it can be re-inserted and the new value is queryable (MVCC re-insert after delete).
def test_mvcc_reinsert_after_delete():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert txn1 doc1 first\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\ndelete txn2 doc1\ncommit txn2", user="alice", store=store)
    run_script("begin txn3\ninsert txn3 doc1 second\ncommit txn3", user="alice", store=store)
    script = """
        begin txn4
        query txn4 second
        commit txn4
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1] and "second" in out[1]

# Test that a delete in a transaction hides the key from queries in the same transaction (logical delete visibility).
def test_mvcc_query_after_delete_same_txn():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert txn1 doc1 gone\ncommit txn1", user="alice", store=store)
    script = """
        begin txn2
        delete txn2 doc1
        query txn2 gone
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" not in out[2]


# Test that after updating and deleting, only the correct keys/values are returned by queries (valid_keys after update/delete).
def test_mvcc_valid_keys_after_update_delete():
    reset_store()
    store = Store()
    run_script("begin txn1\ninsert txn1 doc1 apple\ninsert txn1 doc2 banana\ncommit txn1", user="alice", store=store)
    run_script("begin txn2\nupdate txn2 doc1 orange\ncommit txn2", user="alice", store=store)
    run_script("begin txn3\ndelete txn3 doc2\ncommit txn3", user="alice", store=store)
    script = """
        begin txn4
        query txn4 orange
        commit txn4
    """
    out = run_script(script, user="alice", store=store)
    assert "doc1" in out[1] and "orange" in out[1]
    print("test_mvcc_valid_keys_after_update_delete passed.")

def test_true_snapshot_isolation_with_vector_search():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert txn1 doc1 dog
        insert txn1 doc2 ducks like to eat bread
        insert txn1 doc3 i have a cute dog
        insert txn1 doc4 basketball is life
        insert txn1 doc5 dolphins are smart
        insert txn1 doc6 josh and victor
        insert txn1 doc7 nigerian food is spicy
        commit txn1
        begin txn2
        begin txn3
        update txn3 doc1 cute dog
        commit txn3
        query txn2 cute dogs
        commit txn2
        begin txn4
        query txn4 cute dog
        commit txn4
    """
    out = run_script(script, user="alice", store=store)
    # The order is: [begin txn1, insert x7, commit, begin txn2, begin txn3, update, commit, query txn2, commit, begin txn4, query txn4, commit]
    # out[12] is the result of 'query txn2 cute dogs'
    # out[15] is the result of 'query txn4 cute dog'
    # For txn2, should see the old value 'dog' and 'i have a cute dog'
    result_13 = ast.literal_eval(out[13])
    assert "dog" in result_13.values()
    assert "i have a cute dog" in result_13.values()
    assert "cute dog" not in result_13.values()  # Only fails if the exact value is present
    # For txn4, should see the updated value 'cute dog' and 'i have a cute dog'
    result_16 = ast.literal_eval(out[16])
    assert "cute dog" in result_16.values()
    assert "i have a cute dog" in result_16.values()

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
    test_mvcc_valid_keys_after_update_delete()
    test_true_snapshot_isolation_with_vector_search()
    print("All integration tests passed!")
