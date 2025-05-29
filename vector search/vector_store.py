# vector_store.py

vector_store = {}

def add_vector(key, vector):
    vector_store[key] = vector

def get_all_vectors():
    return [{"key": k, "vector": v} for k, v in vector_store.items()]

def reset_store():
    vector_store.clear()
