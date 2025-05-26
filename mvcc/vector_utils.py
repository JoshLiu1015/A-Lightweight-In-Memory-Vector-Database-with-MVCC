
def convert_text_to_vector(text: str) -> list[float]:
    """Convert input text into a TF-IDF vector."""
    return [0.0]

def send_vector(record_id: str, vector: list[float]) -> None:
    """Placeholder for sending the vector elsewhere (e.g., to a database or ML model)."""
    print(f"[SEND] ID: {record_id}, Vector (first 5 values): {vector}")
