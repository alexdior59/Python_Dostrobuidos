# common/config.py
import os

# PC-B: Nodo Principal (Gestor de Carga + Actores + GA Primario)
GC_IP = "10.43.103.83"
ACTOR_PRESTAMO_IP = "10.43.103.83"
GA_PRIMARY_IP = "10.43.103.83"

# PC-C: Nodo Réplica (GA Secundario)
GA_REPLICA_IP = "10.43.102.197"

# Configuración de Puertos
GC_FRONTEND_PORT = 5555
GC_PUB_PORT = 5556
GA_REQUEST_PORT = 5570
GA_REPL_PUSH_PORT = 5571
GA_REPLICA_REQ_PORT = 5572

# Endpoints ZeroMQ
GC_ENDPOINT = f"tcp://{GC_IP}:{GC_FRONTEND_PORT}"
GC_PUB_ENDPOINT = f"tcp://{GC_IP}:{GC_PUB_PORT}"
ACTOR_PRESTAMO_ENDPOINT = f"tcp://{ACTOR_PRESTAMO_IP}:6000"

GA_PRIMARY_ENDPOINT = f"tcp://{GA_PRIMARY_IP}:{GA_REQUEST_PORT}"
GA_REPLICA_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPLICA_REQ_PORT}"
GA_REPL_PUSH_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPL_PUSH_PORT}"

# Persistencia
DB_PRIMARY_PATH = "biblioteca_primary.json"
DB_REPLICA_PATH = "biblioteca_replica.json"