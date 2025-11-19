# common/config.py

# Durante desarrollo local, todo en 127.0.0.1
GC_IP = "127.0.0.1"
GA_PRIMARY_IP = "127.0.0.1"
GA_REPLICA_IP = "127.0.0.1"

GC_FRONTEND_PORT = 5555      # PS <-> GC
GC_PUB_PORT = 5556           # GC -> Actores (PUB/SUB)
GA_REQUEST_PORT = 5570       # Actores -> GA
GA_REPL_PUSH_PORT = 5571     # GA primary -> GA replica

GC_ENDPOINT = f"tcp://{GC_IP}:{GC_FRONTEND_PORT}"
GC_PUB_ENDPOINT = f"tcp://{GC_IP}:{GC_PUB_PORT}"

GA_PRIMARY_ENDPOINT = f"tcp://{GA_PRIMARY_IP}:{GA_REQUEST_PORT}"
GA_REPLICA_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REQUEST_PORT}"
GA_REPL_PUSH_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPL_PUSH_PORT}"

DB_PRIMARY_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "biblioteca_sd",
    "user": "biblioteca_user",
    "password": "biblioteca123"
}

DB_REPLICA_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "dbname": "biblioteca_sd_replica",
    "user": "biblioteca_user",
    "password": "biblioteca123"
}
