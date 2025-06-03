import os
os.environ["USE_TF"] = "0"

from sentence_transformers import SentenceTransformer
import numpy as np
from scipy.spatial.distance import cdist
from vector_store import get_all_vectors

"""
Utility functions for vector encoding and similarity search.

- string_to_vector: Converts text to a vector using a pre-trained model.
- compute_top_k: Returns the top-k closest vectors to the query.
- get_top_k_keys: Returns just the keys of the top-k matches.
"""

model = SentenceTransformer("hkunlp/instructor-xl")

def string_to_vector(text):
    return model.encode(text).tolist()

def compute_top_k(query, k=5, metric="cosine", valid_keys=None):
    """
    Returns the top-k most similar vectors to the query, with keys and scores.

    Args:
        query (list of float): The query vector.
        k (int): Number of top results to return.
        metric (str): Distance metric to use (default: "cosine").
        valid_keys (list of str): Required. Only these keys will be considered.

    Returns:
        list of dict: Each with 'key' and 'score'.
    """
    if valid_keys is None:
        raise ValueError("valid_keys must be provided.")

    candidates = [c for c in get_all_vectors() if c["key"] in valid_keys]
    if not candidates:
        return []

    keys = [c["key"] for c in candidates]
    vectors = np.array([c["vector"] for c in candidates])
    query_vec = np.array(query)

    distances = cdist([query_vec], vectors, metric=metric)[0]
    top_k_indices = np.argsort(distances)[:k]

    return [
        {"key": keys[i], "score": float(distances[i])}
        for i in top_k_indices
    ]

def get_top_k_keys(query, k=5, metric="cosine", valid_keys=None):
    """
    Returns only the keys of the top-k closest vectors to the query.

    Args:
        query (list of float): The query vector.
        k (int): Number of top results to return.
        metric (str): Distance metric to use.
        valid_keys (list of str): Required.

    Returns:
        list of str: Top-k keys.
    """
    return [r["key"] for r in compute_top_k(query, k=k, metric=metric, valid_keys=valid_keys)]
