from mvcc.cli_core import run_script
from vector_search.vector_store import get_all_vectors, reset_store
from mvcc.store import Store
from vector_search.utils import string_to_vector, compute_top_k

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
    """
    run_script(script, user="alice", store=store)
    query_vec = string_to_vector("Apple announces new smartphone")
    results = compute_top_k(query_vec, get_all_vectors(), k=2)
    print("Vector search results:", results)
    assert any(r["key"].startswith("doc2") for r in results)


if __name__ == "__main__":
    test_mvcc_insert_triggers_vector_store()
    test_mvcc_query_vector_search()
    print("All integration tests passed!")
