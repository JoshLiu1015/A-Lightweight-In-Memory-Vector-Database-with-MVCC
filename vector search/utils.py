import os
os.environ["USE_TF"] = "0"

from sentence_transformers import SentenceTransformer
import numpy as np
from scipy.spatial.distance import cdist

"""
Utility functions for vector encoding and similarity search.

- string_to_vector: Converts text to a vector using a pre-trained model.
- compute_top_k: Finds the top-k closest vectors to a query using a distance metric.
"""


model = SentenceTransformer("hkunlp/instructor-xl")

def string_to_vector(text):
    return model.encode(text).tolist()

def compute_top_k(query, candidates, k=5, metric="cosine"):
    keys = [c["key"] for c in candidates]
    vectors = np.array([c["vector"] for c in candidates])
    query_vec = np.array(query)

    distances = cdist([query_vec], vectors, metric=metric)[0]
    top_k_indices = np.argsort(distances)[:k]

    return [
        {"key": keys[i], "score": float(distances[i])}
        for i in top_k_indices
    ]

def get_top_k_keys(query_vector, candidates, k=5, metric="cosine"):
    """
    Returns only the keys of the top-k closest vectors to the query.

    Args:
        query_vector (list of float): The query embedding.
        candidates (list of dict): Each dict should have 'key' and 'vector'.
        k (int): Number of top results to return.
        metric (str): Similarity metric (default: "cosine").

    Returns:
        list of str: Keys of the top-k most similar vectors.
    """
    top_k_results = compute_top_k(query_vector, candidates, k=k, metric=metric)
    return [result["key"] for result in top_k_results]
