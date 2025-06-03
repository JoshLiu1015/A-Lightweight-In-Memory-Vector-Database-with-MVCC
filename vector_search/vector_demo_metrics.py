import time
import matplotlib.pyplot as plt
from utils import string_to_vector, compute_top_k
from vector_store import reset_store, add_vector, get_all_vectors

def generate_samples(prefix, count):
    return [
        (f"{prefix}_{i}", f"{prefix} article number {i} discusses something interesting.")
        for i in range(count)
    ]

def benchmark_vector_search(sizes):
    insert_times = []
    search_all_times = []
    search_filtered_times = []

    for size in sizes:
        reset_store()

        # Generate and insert vectors
        samples = generate_samples("news", size // 2) + generate_samples("health", size // 2)
        start_insert = time.time()
        for key, text in samples:
            vec = string_to_vector(text)
            add_vector(key, vec)
        insert_duration = time.time() - start_insert
        insert_times.append(insert_duration)

        # Prepare query vector
        query = "latest research on public health policies"
        query_vec = string_to_vector(query)

        # Top-k over all vectors
        all_keys = [v["key"] for v in get_all_vectors()]
        start_all = time.time()
        compute_top_k(query_vec, k=5, valid_keys=all_keys)
        search_all_times.append(time.time() - start_all)

        # Top-k over filtered vectors
        valid_keys = [f"health_{i}" for i in range(size // 2)]
        start_filtered = time.time()
        compute_top_k(query_vec, k=5, valid_keys=valid_keys)
        search_filtered_times.append(time.time() - start_filtered)

    return sizes, insert_times, search_all_times, search_filtered_times

sizes, insert_times, all_times, filtered_times = benchmark_vector_search([500, 1000, 2000])

# Plotting
plt.figure(figsize=(10, 6))
plt.plot(sizes, insert_times, label="Insertion Time")
plt.plot(sizes, all_times, label="Top-k Over All Vectors")
plt.plot(sizes, filtered_times, label="Top-k Over Filtered Vectors")
plt.xlabel("Number of Vectors")
plt.ylabel("Time (seconds)")
plt.title("Performance Metrics vs. Dataset Size")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
