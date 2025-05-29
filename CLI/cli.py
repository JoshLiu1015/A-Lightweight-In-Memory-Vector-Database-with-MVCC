import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from mvcc.store import Store
from mvcc.record import Record


def main():
    store = Store()
    current_txn = None
    
    print("Vector DB CLI started. Commands:")
    print("  begin - Start a new transaction")
    print("  commit - Commit current transaction")
    print("  insert <id> <value> - Insert a record")
    print("  update <id> <value> - Update a record")
    print("  delete <id> - Delete a record")
    print("  query - Query all records")
    print("  exit - Exit the program")
    
    while True:
        cmd = input(">>> ").strip().split()
        if not cmd:
            continue

        action = cmd[0].lower()

        if action == "exit":
            break

        try:
            if action == "begin":
                if current_txn is not None:
                    print("Error: Transaction already in progress")
                    continue
                current_txn = store.begin_transaction()
                print(f"Started transaction {current_txn}")
                
            elif action == "commit":
                if current_txn is None:
                    print("Error: No transaction in progress")
                    continue
                store.commit_transaction(current_txn)
                print(f"Committed transaction {current_txn}")
                current_txn = None

            elif action == "insert":
                if current_txn is None:
                    print("Error: No transaction in progress. Use 'begin' first.")
                    continue
                if len(cmd) < 3:
                    print("Usage: insert <id> <value>")
                    continue
                r = Record(cmd[1], " ".join(cmd[2:]))
                store.insert(current_txn, r)
                print(f"Inserted {cmd[1]} with value '{r.value}'")

            elif action == "update":
                if current_txn is None:
                    print("Error: No transaction in progress. Use 'begin' first.")
                    continue
                if len(cmd) < 3:
                    print("Usage: update <id> <value>")
                    continue
                r = Record(cmd[1], " ".join(cmd[2:]))
                store.update(current_txn, r)
                print(f"Updated {cmd[1]} with value '{r.value}'")

            elif action == "delete":
                if current_txn is None:
                    print("Error: No transaction in progress. Use 'begin' first.")
                    continue
                if len(cmd) != 2:
                    print("Usage: delete <id>")
                    continue
                store.delete(current_txn, cmd[1])
                print(f"Deleted {cmd[1]}")

            elif action == "query":
                if current_txn is None:
                    print("Error: No transaction in progress. Use 'begin' first.")
                    continue
                results = store.read(current_txn)
                print(f"Query Results (txn {current_txn}):")
                for r in results:
                    print(f"  {r.id} → {r.value} (version {r.key})")

            else:
                print("Invalid command.")

        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
