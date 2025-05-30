from utils import string_to_vector, compute_top_k, get_top_k_keys
from vector_store import reset_store, add_vector, get_all_vectors

"""
Unit tests for vector search utilities and vector store functionality.
"""


def test_string_to_vector_returns_vector():
    # Tests that string_to_vector returns a list of floats for a given text input.
    vec = string_to_vector("finance: Apple stock rises")
    assert isinstance(vec, list)
    assert all(isinstance(x, float) for x in vec)

def test_compute_top_k_returns_top_matches():
    # Tests that compute_top_k returns the correct number and format of top matches.
    # Sets up a vector store with three vectors, queries for the top 2 matches,
    # and checks the result structure.
    reset_store()
    add_vector("x", string_to_vector("finance: Inflation is rising"))
    add_vector("y", string_to_vector("finance: Interest rates are high"))
    add_vector("z", string_to_vector("finance: Apple releases earnings"))

    query_vec = string_to_vector("finance: Fed hikes interest rates")
    results = compute_top_k(query_vec, get_all_vectors(), k=2)

    assert isinstance(results, list)
    assert len(results) == 2
    assert all("key" in r and "score" in r for r in results)


from utils import string_to_vector, get_top_k_keys
from vector_store import reset_store, add_vector, get_all_vectors

def test_get_top_k_keys_returns_keys_only():
    # Arrange: clear store and add example vectors
    reset_store()
    add_vector("a", string_to_vector("finance: Stock prices soar"))
    add_vector("b", string_to_vector("finance: Interest rates cut"))
    add_vector("c", string_to_vector("finance: Economic outlook weakens"))

    # Convert query string to vector
    query_vector = string_to_vector("finance: Market rally continues")

    # Act: get top-k matching keys using the query vector
    keys = get_top_k_keys(query_vector, get_all_vectors(), k=2)

    # Assert: results are valid keys from the store
    assert isinstance(keys, list)
    assert len(keys) == 2
    assert all(isinstance(k, str) for k in keys)
    assert all(k in ["a", "b", "c"] for k in keys)
