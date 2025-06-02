import os
os.environ["USE_TF"] = "0"

from sentence_transformers import SentenceTransformer
import numpy as np
from scipy.spatial.distance import cdist
from vector_search.vector_store import get_all_vectors

"""
Utility functions for vector encoding and similarity search.

- string_to_vector: Converts text to a vector using a pre-trained model.
- compute_top_k: Finds the top-k closest vectors to a query using a distance metric.
"""


model = SentenceTransformer("hkunlp/instructor-xl")

def string_to_vector(text):
    return model.encode(text).tolist()

def get_top_k_keys(query, valid_keys, k, metric="cosine"):
    """
    Computes the top-k closest vectors to the query.

    Args:
        query (list of float): The query vector.
        k (int): Number of top results to return.
        metric (str): Distance metric to use (default: "cosine").
        valid_keys (list of str, optional): If provided, only consider candidates whose keys are in this list.

    Returns:
        list of dict: Top-k results with 'key' and 'score'.
    """

    keys_and_vectors = get_all_vectors()
    # print(f"Total vectors in store: {len(keys_and_vectors)}")
    
    candidates = [c for c in keys_and_vectors if c["key"] in valid_keys]
    # print(f"Filtered candidates: {len(candidates)}")

    if not candidates:
        return []

    keys = [c["key"] for c in candidates]
    vectors = np.array([c["vector"] for c in candidates])
    query_vec = np.array(query)

    distances = cdist([query_vec], vectors, metric=metric)[0]
    # print(f"Distances: {distances}")

    top_k_indices = np.argsort(distances)[:k]
    # print(f"Top {k} indices: {top_k_indices}")

    return [keys[i] for i in top_k_indices]
