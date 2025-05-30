from utils import string_to_vector, compute_top_k
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
    results = compute_top_k(query_vec, get_all_vectors(), k=2)

    assert isinstance(results, list)
    assert len(results) == 2
    assert all("key" in r and "score" in r for r in results)
