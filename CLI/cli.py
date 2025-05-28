import sys
import os
import ast
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")


from mvcc.store import Store
from mvcc.record import Record


def main():
    store = Store()
    
    print("Vector DB CLI started. Commands: insert <id>, update <id>, delete <id>, query, exit")
    
    while True:
        cmd = input(">>> ").strip().split()
        if not cmd:
            continue

        action = cmd[0].lower()

        if action == "exit":
            break

        txn_id = store.begin_transaction()

        try:
            if action == "insert" and len(cmd) == 2:
                r = Record(cmd[1])
                store.insert(txn_id, r)
                store.commit_transaction(txn_id)
                print(f"Inserted {cmd[1]}")
            
            elif action == "insert" and len(cmd) >= 3:
                try:
                    vec = ast.literal_eval(" ".join(cmd[2:]))  # safely parse the list
                    if not isinstance(vec, list) or not all(isinstance(x, (float, int)) for x in vec):
                        raise ValueError("Vector must be a list of numbers.")
                except Exception as e:
                    print(f"Invalid vector format: {e}")
                    continue

                r = Record(cmd[1])
                r.vector = vec
                store.insert(txn_id, r)
                store.commit_transaction(txn_id)
                print(f"Inserted {cmd[1]} with vector {vec}")


            elif action == "update" and len(cmd) == 2:
                r = Record(cmd[1])
                store.update(txn_id, r)
                store.commit_transaction(txn_id)
                print(f"Updated {cmd[1]}")

            elif action == "delete" and len(cmd) == 2:
                store.delete(txn_id, cmd[1])
                store.commit_transaction(txn_id)
                print(f"Deleted {cmd[1]}")

            elif action == "query":
                results = store.read(txn_id)
                print(f"Query Results (txn {txn_id}):")
                for r in results:
                    print(f"  {r.id} → version {r.key}")
                store.commit_transaction(txn_id)

            else:
                print("Invalid command.")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
