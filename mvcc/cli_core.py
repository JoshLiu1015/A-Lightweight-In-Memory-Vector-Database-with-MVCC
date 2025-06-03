from mvcc.store import Store
from mvcc.record import Record

class Shell:
    def __init__(self, user, store=None):
        self.user = user
        self.store = store 
        self.current_txn = None

def process_line(shell, line):
    cmd, *args = line.strip().split()
    if cmd == "begin":
        shell.current_txn = shell.store.begin_transaction()
        return f"began T{shell.current_txn}"
    elif cmd == "insert":
        key, value = args[0], " ".join(args[1:])
        shell.store.insert(shell.current_txn, Record(key, value))
        return "ok"
    elif cmd == "update":
        key, value = args[0], " ".join(args[1:])
        shell.store.update(shell.current_txn, Record(key, value))
        return "ok"
    elif cmd == "delete":
        key = args[0]
        shell.store.delete(shell.current_txn, key)
        return "ok"
    elif cmd == "commit":
        shell.store.commit_transaction(shell.current_txn)
        shell.current_txn = None
        return "committed"
    elif cmd == "query":
        # If a query string is provided, use it; otherwise, use empty string
        query_str = " ".join(args) if args else ""
        return repr({r.id: r.value for r in shell.store.read(shell.current_txn, query_str, 100)})
    else:
        return f"Unknown command: {cmd}"

def run_script(script, user="anon", store=None):
    shell = Shell(user, store)
    outputs = []
    for line in script.strip().splitlines():
        outputs.append(process_line(shell, line))
    return outputs
