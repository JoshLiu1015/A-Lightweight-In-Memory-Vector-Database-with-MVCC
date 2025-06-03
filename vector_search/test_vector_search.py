from utils import string_to_vector, compute_top_k, get_top_k_keys
from vector_store import reset_store, add_vector, get_all_vectors

"""
Unit tests for vector search utilities and vector store functionality.
"""

def test_string_to_vector_returns_vector():
    vec = string_to_vector("finance: Apple stock rises")
    assert isinstance(vec, list)
    assert all(isinstance(x, float) for x in vec)

def test_compute_top_k_returns_top_matches():
    reset_store()
    add_vector("x", string_to_vector("finance: Inflation is rising"))
    add_vector("y", string_to_vector("finance: Interest rates are high"))
    add_vector("z", string_to_vector("finance: Apple releases earnings"))

    query_vec = string_to_vector("finance: Fed hikes interest rates")
    valid_keys = ["x", "y", "z"]
    results = compute_top_k(query_vec, k=2, valid_keys=valid_keys)

    assert isinstance(results, list)
    assert len(results) == 2
    assert all("key" in r and "score" in r for r in results)

def test_get_top_k_keys_returns_keys_only():
    reset_store()
    add_vector("a", string_to_vector("finance: Stock prices soar"))
    add_vector("b", string_to_vector("finance: Interest rates cut"))
    add_vector("c", string_to_vector("finance: Economic outlook weakens"))

    query_vector = string_to_vector("finance: Market rally continues")
    valid_keys = ["a", "b", "c"]
    keys = get_top_k_keys(query_vector, k=2, valid_keys=valid_keys)

    assert isinstance(keys, list)
    assert len(keys) == 2
    assert all(isinstance(k, str) for k in keys)
    assert all(k in ["a", "b", "c"] for k in keys)

def test_compute_top_k_with_valid_keys_filter():
    reset_store()
    add_vector("1", string_to_vector("health: sleep affects heart health"))
    add_vector("2", string_to_vector("sports: NBA season starts strong"))
    add_vector("3", string_to_vector("health: doctors link sleep to longevity"))

    query_vec = string_to_vector("study finds sleep improves cardiovascular health")
    results = compute_top_k(query_vec, k=2, valid_keys=["1", "3"])

    assert len(results) == 2
    assert all(r["key"] in ["1", "3"] for r in results)

def test_key_overwrite():
    reset_store()
    add_vector("doc1", string_to_vector("first version"))
    add_vector("doc1", string_to_vector("second version"))  # should overwrite
    all_keys = ["doc1"]
    query_vec = string_to_vector("second version")
    results = compute_top_k(query_vec, k=1, valid_keys=all_keys)
    assert results[0]["key"] == "doc1"

def test_search_with_no_data():
    reset_store()
    query_vec = string_to_vector("anything")
    results = compute_top_k(query_vec, k=3, valid_keys=[])
    assert results == []

def test_search_with_invalid_valid_keys():
    reset_store()
    add_vector("doc1", string_to_vector("foo"))
    add_vector("doc2", string_to_vector("bar"))
    query_vec = string_to_vector("baz")
    results = compute_top_k(query_vec, k=2, valid_keys=["nonexistent1", "nonexistent2"])
    assert results == []

def test_multiple_searches():
    reset_store()
    add_vector("doc1", string_to_vector("alpha beta gamma"))
    add_vector("doc2", string_to_vector("delta epsilon zeta"))
    query_vec1 = string_to_vector("alpha")
    query_vec2 = string_to_vector("zeta")
    results1 = compute_top_k(query_vec1, k=1, valid_keys=["doc1", "doc2"])
    results2 = compute_top_k(query_vec2, k=1, valid_keys=["doc1", "doc2"])
    assert results1[0]["key"] == "doc1"
    assert results2[0]["key"] == "doc2"

def test_duplicate_vectors_different_keys():
    reset_store()
    vec = string_to_vector("identical content")
    add_vector("docA", vec)
    add_vector("docB", vec)
    query_vec = string_to_vector("identical content")
    results = compute_top_k(query_vec, k=2, valid_keys=["docA", "docB"])
    keys = [r["key"] for r in results]
    assert "docA" in keys and "docB" in keys

if __name__ == "__main__":
    test_string_to_vector_returns_vector()
    test_compute_top_k_returns_top_matches()
    test_get_top_k_keys_returns_keys_only()
    test_compute_top_k_with_valid_keys_filter()
    test_key_overwrite()
    test_search_with_no_data()
    test_search_with_invalid_valid_keys()
    test_multiple_searches()
    test_duplicate_vectors_different_keys()
    print("All vector search tests passed!")
