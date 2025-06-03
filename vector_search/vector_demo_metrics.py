import time
from utils import string_to_vector, compute_top_k
from vector_store import reset_store, add_vector, get_all_vectors

def generate_samples(prefix, count):
    return [
        (f"{prefix}_{i}", f"{prefix} article number {i} discusses something interesting.")
        for i in range(count)
    ]

def demo_performance_metrics():
    reset_store()

    # --- Step 1: Generate and Insert Vectors ---
    print("Generating and inserting 1000 vectors...")
    start_insert = time.time()

    samples = generate_samples("news", 500) + generate_samples("health", 500)
    for key, text in samples:
        vec = string_to_vector(text)
        add_vector(key, vec)

    insert_time = time.time() - start_insert
    print(f"Insertion of 1000 vectors took: {insert_time:.2f} seconds\n")

    # --- Step 2: Prepare a Query Vector ---
    query = "latest research on public health policies"
    query_vec = string_to_vector(query)

    # --- Step 3: compute_top_k over ALL vectors ---
    print("Computing top-k over all vectors...")
    start_all = time.time()
    results_all = compute_top_k(query_vec, k=5, valid_keys=[v["key"] for v in get_all_vectors()])
    time_all = time.time() - start_all
    print(f"Top-k search over all vectors took: {time_all:.2f} seconds")
    print("Results:", results_all, "\n")

    # --- Step 4: compute_top_k over FILTERED keys ---
    valid_keys = [f"health_{i}" for i in range(500)]
    print("Computing top-k with valid_keys filter (500 keys)...")
    start_filtered = time.time()
    results_filtered = compute_top_k(query_vec, k=5, valid_keys=valid_keys)
    time_filtered = time.time() - start_filtered
    print(f"Top-k search with filter took: {time_filtered:.2f} seconds")
    print("Results:", results_filtered)

if __name__ == "__main__":
    demo_performance_metrics()
