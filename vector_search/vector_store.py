# vector_store.py

"""
In-memory key-vector store utilities.

- add_vector: Adds a vector with a key.
- get_all_vectors: Retrieves all stored vectors.
- reset_store: Clears the store.
"""


vector_store = {}

def add_vector(key, vector):
    vector_store[key] = vector

def get_all_vectors():
    return [{"key": k, "vector": v} for k, v in vector_store.items()]

def reset_store():
    vector_store.clear()
