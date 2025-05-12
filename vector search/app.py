from flask import Flask, request, jsonify
from utils import compute_top_k

app = Flask(__name__)

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    try:
        query_vector = data["query"]
        candidates = data["candidates"]
        k = int(data.get("k", 5))
        metric = data.get("metric", "cosine")

        results = compute_top_k(query_vector, candidates, k=k, metric=metric)
        return jsonify(results)

    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(port=5000)
