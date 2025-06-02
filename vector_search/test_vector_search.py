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
