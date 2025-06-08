from CLI.cli_core import run_script
from vector_search.vector_store import reset_store
from mvcc.store import Store
import ast


# Test that a query returns the correct document based on vector similarity after insertion and commit.
def test_mvcc_query_vector_search():
    reset_store()
    store = Store()
    script = """
        begin txn1
        insert txn1 doc1 The quick brown fox jumps over the lazy dog
        insert txn1 doc2 Apple unveils new iPhone at tech event
        insert txn1 doc3 Apple smartphone review
        commit txn1
        begin txn2
        query txn2 Apple smartphone
        commit txn2
    """
    out = run_script(script, user="alice", store=store)
    assert "doc2" in out[-2] and "doc3" in out[-2]



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
    assert "dog" not in result_16.values()  # Only fails if the old value is present

if __name__ == "__main__":
    test_mvcc_query_vector_search()
    test_true_snapshot_isolation_with_vector_search()
    print("All integration tests passed!")
