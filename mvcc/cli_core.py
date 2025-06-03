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
        key, value = args[0], " ".join(args[1:])
        shell.store.insert(shell.current_txn, Record(key, value))
        return "ok"
    elif cmd == "update":
        txn_name = args[0]
        key, value = args[1], " ".join(args[2:])
        txn_id = shell.txn_map[txn_name]
        shell.store.update(txn_id, Record(key, value))
        return "ok"
    elif cmd == "delete":
        key = args[0]
        shell.store.delete(shell.current_txn, key)
        return "ok"
    elif cmd == "commit":
        txn_name = args[0] if args else "default"
        txn_id = shell.txn_map.get(txn_name, shell.current_txn)
        shell.store.commit_transaction(txn_id)
        return f"committed {txn_name} T{txn_id}"
    elif cmd == "query":
        # If a txn name is provided, use it; otherwise, use current_txn
        if args:
            txn_name = args[0]
            txn_id = shell.txn_map.get(txn_name, shell.current_txn)
            query_str = " ".join(args[1:]) if len(args) > 1 else ""
        else:
            txn_id = shell.current_txn
            query_str = ""
        return repr({r.id: r.value for r in shell.store.read(txn_id, query_str, 100)})
    else:
        return f"Unknown command: {cmd}"

def run_script(script, user="anon", store=None):
    shell = Shell(user, store)
    outputs = []
    for line in script.strip().splitlines():
        outputs.append(process_line(shell, line))
    return outputs
