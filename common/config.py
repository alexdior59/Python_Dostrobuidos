# common/config.py
import os

# ==========================================
# CONFIGURACIÓN DE RED (2 MAQUINAS)
# ==========================================

# MAQUINA 1: Nodo Principal (10.43.102.88)
# Ejecuta: Gestor de Carga, Actores, GA Primario
GC_IP = "10.43.102.88"
ACTOR_PRESTAMO_IP = "10.43.102.88"
GA_PRIMARY_IP = "10.43.102.88"

# MAQUINA 2: Nodo Réplica (10.43.102.197)
# Ejecuta: GA Réplica
GA_REPLICA_IP = "10.43.102.197"

# ==========================================
# PUERTOS Y ENDPOINTS (NO TOCAR)
# ==========================================

GC_FRONTEND_PORT = 5555
GC_PUB_PORT = 5556
GA_REQUEST_PORT = 5570
GA_REPL_PUSH_PORT = 5571
GA_REPLICA_REQ_PORT = 5572

GC_ENDPOINT = f"tcp://{GC_IP}:{GC_FRONTEND_PORT}"
GC_PUB_ENDPOINT = f"tcp://{GC_IP}:{GC_PUB_PORT}"
ACTOR_PRESTAMO_ENDPOINT = f"tcp://{ACTOR_PRESTAMO_IP}:6000"

GA_PRIMARY_ENDPOINT = f"tcp://{GA_PRIMARY_IP}:{GA_REQUEST_PORT}"
GA_REPLICA_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPLICA_REQ_PORT}"
GA_REPL_PUSH_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPL_PUSH_PORT}"

DB_PRIMARY_PATH = "biblioteca_primary.json"
DB_REPLICA_PATH = "biblioteca_replica.json"