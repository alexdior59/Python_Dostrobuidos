# common/config.py
import os

# IPS
GC_IP = "127.0.0.1"
ACTOR_PRESTAMO_IP = "127.0.0.1"
GA_PRIMARY_IP = "127.0.0.1"
GA_REPLICA_IP = "127.0.0.1"

# PUERTOS
GC_FRONTEND_PORT = 5555
GC_PUB_PORT = 5556
GA_REQUEST_PORT = 5570      # Puerto del Primario
GA_REPL_PUSH_PORT = 5571    # Puerto de replicación interna
GA_REPLICA_REQ_PORT = 5572  # <--- NUEVO: Puerto de respaldo para la réplica

# ENDPOINTS
GC_ENDPOINT = f"tcp://{GC_IP}:{GC_FRONTEND_PORT}"
GC_PUB_ENDPOINT = f"tcp://{GC_IP}:{GC_PUB_PORT}"
ACTOR_PRESTAMO_ENDPOINT = f"tcp://{ACTOR_PRESTAMO_IP}:6000"

GA_PRIMARY_ENDPOINT = f"tcp://{GA_PRIMARY_IP}:{GA_REQUEST_PORT}"

# CAMBIO AQUÍ: Usamos el nuevo puerto 5572 para que los actores busquen aquí si falla el primario
GA_REPLICA_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPLICA_REQ_PORT}"

GA_REPL_PUSH_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPL_PUSH_PORT}"

# RUTAS JSON
DB_PRIMARY_PATH = "biblioteca_primary.json"
DB_REPLICA_PATH = "biblioteca_replica.json"