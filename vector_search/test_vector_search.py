from utils import string_to_vector, compute_top_k, get_top_k_keys
from vector_store import reset_store, add_vector, get_all_vectors

"""
Unit tests for vector search utilities and vector store functionality.
"""


def test_string_to_vector_returns_vector():
    # Test that string_to_vector returns a list of floats for a given text input.
    vec = string_to_vector("finance: Apple stock rises")
    assert isinstance(vec, list)
    assert all(isinstance(x, float) for x in vec)


def test_compute_top_k_returns_top_matches():
    # Test that compute_top_k returns the correct number and structure of top matches.
    reset_store()
    add_vector("x", string_to_vector("finance: Inflation is rising"))
    add_vector("y", string_to_vector("finance: Interest rates are high"))
    add_vector("z", string_to_vector("finance: Apple releases earnings"))

    query_vec = string_to_vector("finance: Fed hikes interest rates")
    results = compute_top_k(query_vec, get_all_vectors(), k=2)

    assert isinstance(results, list)
    assert len(results) == 2
    assert all("key" in r and "score" in r for r in results)


def test_get_top_k_keys_returns_keys_only():
    # Test that get_top_k_keys returns only keys from top-k matches.
    reset_store()
    add_vector("a", string_to_vector("finance: Stock prices soar"))
    add_vector("b", string_to_vector("finance: Interest rates cut"))
    add_vector("c", string_to_vector("finance: Economic outlook weakens"))

    query_vector = string_to_vector("finance: Market rally continues")
    keys = get_top_k_keys(query_vector, get_all_vectors(), k=2)

    assert isinstance(keys, list)
    assert len(keys) == 2
    assert all(isinstance(k, str) for k in keys)
    assert all(k in ["a", "b", "c"] for k in keys)


def test_compute_top_k_with_valid_keys_filter():
    # Test that compute_top_k respects the valid_keys filter.
    reset_store()
    add_vector("1", string_to_vector("health: sleep affects heart health"))
    add_vector("2", string_to_vector("sports: NBA season starts strong"))
    add_vector("3", string_to_vector("health: doctors link sleep to longevity"))

    query_vec = string_to_vector("study finds sleep improves cardiovascular health")
    all_vectors = get_all_vectors()

    filtered_results = compute_top_k(query_vec, all_vectors, k=2, valid_keys=["1", "3"])

    assert len(filtered_results) == 2
    assert all(r["key"] in ["1", "3"] for r in filtered_results)


def test_key_overwrite():
    # Test that inserting a vector with the same key overwrites the previous value.
    reset_store()
    add_vector("doc1", string_to_vector("first version"))
    add_vector("doc1", string_to_vector("second version"))
    all_vectors = get_all_vectors()
    assert len(all_vectors) == 1
    # Query with a string similar to the second version
    query_vec = string_to_vector("second version")
    results = compute_top_k(query_vec, all_vectors, k=1)
    assert results[0]["key"] == "doc1"


def test_search_with_no_data():
    # Test that searching with no data returns an empty result.
    reset_store()
    query_vec = string_to_vector("anything")
    results = compute_top_k(query_vec, get_all_vectors(), k=3)
    assert results == []


def test_search_with_invalid_valid_keys():
    # Test that searching with valid_keys that don't exist returns an empty list.
    reset_store()
    add_vector("doc1", string_to_vector("foo"))
    add_vector("doc2", string_to_vector("bar"))
    query_vec = string_to_vector("baz")
    results = compute_top_k(query_vec, get_all_vectors(), k=2, valid_keys=["nonexistent1", "nonexistent2"])
    assert results == []


def test_multiple_searches():
    # Test that multiple searches in the same session are consistent.
    reset_store()
    add_vector("doc1", string_to_vector("alpha beta gamma"))
    add_vector("doc2", string_to_vector("delta epsilon zeta"))
    query_vec1 = string_to_vector("alpha")
    query_vec2 = string_to_vector("zeta")
    results1 = compute_top_k(query_vec1, get_all_vectors(), k=1)
    results2 = compute_top_k(query_vec2, get_all_vectors(), k=1)
    assert results1[0]["key"] == "doc1"
    assert results2[0]["key"] == "doc2"


def test_duplicate_vectors_different_keys():
    # Test that inserting the same vector under two different keys allows both to appear in top-k.
    reset_store()
    vec = string_to_vector("identical content")
    add_vector("docA", vec)
    add_vector("docB", vec)
    query_vec = string_to_vector("identical content")
    results = compute_top_k(query_vec, get_all_vectors(), k=2)
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
