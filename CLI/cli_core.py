from mvcc.store import Store
from mvcc.record import Record

class Shell:
    def __init__(self, user, store=None):
        self.user = user
        self.store = store
        self.current_txn = None
        self.txn_map = {}  # Maps user txn names to actual txn IDs

def process_line(shell, line):
    cmd, *args = line.strip().split()
    if cmd == "begin":
        txn_name = args[0] if args else "default"
        txn_id = shell.store.begin_transaction()
        shell.txn_map[txn_name] = txn_id
        shell.current_txn = txn_id  # Optionally set as current
        return f"began {txn_name} T{txn_id}"
    elif cmd == "insert":
        txn_name = args[0]
        key, value = args[1], " ".join(args[2:])
        txn_id = shell.txn_map[txn_name]
        shell.store.insert(txn_id, Record(key, value))
        return "ok"
    elif cmd == "update":
        txn_name = args[0]
        key, value = args[1], " ".join(args[2:])
        txn_id = shell.txn_map[txn_name]
        returnMessage = shell.store.update(txn_id, Record(key, value))
        if returnMessage == "The update was blocked by an existing version, but it has been applied now.":
            return "blocked, but applied"
        else:
            return "ok"
    elif cmd == "delete":
        txn_name = args[0]
        key = args[1]
        txn_id = shell.txn_map[txn_name]
        shell.store.delete(txn_id, key)
        return "ok"
    elif cmd == "commit":
        txn_name = args[0] if args else "default"
        txn_id = shell.txn_map.get(txn_name, shell.current_txn)
        shell.store.commit_transaction(txn_id)
        return f"committed {txn_name} T{txn_id}"
    elif cmd == "abort":
        txn_name = args[0] if args else "default"
        txn_id = shell.txn_map.get(txn_name, shell.current_txn)
        shell.store.abort_transaction(txn_id)
        return f"aborted {txn_name} T{txn_id}"
    elif cmd == "query":
        # If a txn name is provided, use it; otherwise, use current_txn
        if args:
            txn_name = args[0]
            txn_id = shell.txn_map.get(txn_name, shell.current_txn)
            query_str = " ".join(args[1:]) if len(args) > 1 else ""
        else:
            txn_id = shell.current_txn
            query_str = ""
        print("txn_id:", txn_id, "query_str:", query_str)
        return repr({r.id: r.value for r in shell.store.read(txn_id, query_str, 2)})
    elif cmd == "sleep":
        import time
        time.sleep(5)
    else:
        return f"Unknown command: {cmd}"

def run_script(script, user="anon", store=None):
    shell = Shell(user, store)
    outputs = []
    for line in script.strip().splitlines():
        outputs.append(process_line(shell, line))
    return outputs
