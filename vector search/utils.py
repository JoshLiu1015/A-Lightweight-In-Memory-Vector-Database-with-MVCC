import numpy as np
from scipy.spatial.distance import cdist

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
