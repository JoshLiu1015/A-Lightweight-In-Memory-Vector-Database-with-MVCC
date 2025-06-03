import sys
from mvcc.cli_core import Shell, process_line
from mvcc.store import get_store

def main():
    store = get_store()
    user = input("User name: ")
    shell = Shell(user, store)
    while True:
        out = process_line(shell, input(f"{user}> "))
        print(out)

if __name__ == "__main__":
    main()
