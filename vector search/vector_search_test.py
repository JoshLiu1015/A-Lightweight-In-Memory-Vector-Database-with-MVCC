from utils import string_to_vector, compute_top_k, get_top_k_keys
from vector_store import reset_store, add_vector, get_all_vectors

# Reset the in-memory store
reset_store()

# Print embedding of a test string
print("=== Embedding for sample string ===")
print(string_to_vector("hello world, it's a beautiful day!"))
print()

# Add some example vectors
print("=== Adding vectors to store ===")
add_vector("doc1", string_to_vector("sports: NBA playoffs continue into finals weekend"))
add_vector("doc2", string_to_vector("tech: Apple unveils its latest AR headset"))
add_vector("doc3", string_to_vector("fitness: research links poor sleep to increased leg disease risk"))
add_vector("doc4", string_to_vector("entertainment: Taylor Swift announces new world tour dates"))
add_vector("doc5", string_to_vector("science: NASA’s James Webb discovers water on distant planet"))
add_vector("doc6", string_to_vector("finance: inflation cools, Fed signals rate pause"))
add_vector("doc7", string_to_vector("health: doctors recommend 7–9 hours of sleep for cardiovascular"))
print(get_all_vectors())
print()

# Create a query vector
query = "body: new study finds link between snooze and feet health"
query_vec = string_to_vector(query)
print("=== Query vector ===")
print(query_vec)
print()

# Compute top-k matches
print("=== Top-k results ===")
top_k_results = compute_top_k(query_vec, get_all_vectors(), k=2)
for result in top_k_results:
    print(f"Key: {result['key']}, Score: {result['score']:.4f}")
print()

# Print just the keys from the top-k results
print("=== Top-k keys only ===")
top_k_keys = get_top_k_keys(query_vec, get_all_vectors(), k=2)
print(top_k_keys)
