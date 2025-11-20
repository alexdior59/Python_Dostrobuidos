# common/config.py

# CONFIGURACION DE IPS (Cambia esto según tus máquinas reales)
GC_IP = "127.0.0.1"          # Máquina donde corre el Gestor de Carga
ACTOR_PRESTAMO_IP = "127.0.0.1" # Máquina donde corre el Actor Préstamo (suele ser la misma que GC)
GA_PRIMARY_IP = "127.0.0.1"  # Máquina B (Primario)
GA_REPLICA_IP = "127.0.0.1"  # Máquina C (Réplica)

# PUERTOS
GC_FRONTEND_PORT = 5555      # PS <-> GC
GC_PUB_PORT = 5556           # GC -> Actores (PUB/SUB)
GA_REQUEST_PORT = 5570       # Actores -> GA (El puerto que intentan contactar)
GA_REPL_PUSH_PORT = 5571     # GA primary -> GA replica

# ENDPOINTS (Se generan automáticamente con las IPs de arriba)
GC_ENDPOINT = f"tcp://{GC_IP}:{GC_FRONTEND_PORT}"
GC_PUB_ENDPOINT = f"tcp://{GC_IP}:{GC_PUB_PORT}"

# Endpoint específico para que el GC encuentre al Actor Préstamo
ACTOR_PRESTAMO_ENDPOINT = f"tcp://{ACTOR_PRESTAMO_IP}:6000"

GA_PRIMARY_ENDPOINT = f"tcp://{GA_PRIMARY_IP}:{GA_REQUEST_PORT}"
GA_REPLICA_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REQUEST_PORT}"
GA_REPL_PUSH_ENDPOINT = f"tcp://{GA_REPLICA_IP}:{GA_REPL_PUSH_PORT}"

# Configuración de Base de Datos
DB_PRIMARY_CONFIG = {
    "host": "127.0.0.1", # Cambiar a la IP de la DB Primaria si es externa
    "port": 5432,
    "dbname": "biblioteca_sd",
    "user": "biblioteca_user",
    "password": "biblioteca123"
}

DB_REPLICA_CONFIG = {
    "host": "127.0.0.1", # Cambiar a la IP de la DB Réplica si es externa
    "port": 5432,
    "dbname": "biblioteca_sd_replica",
    "user": "biblioteca_user",
    "password": "biblioteca123"
}