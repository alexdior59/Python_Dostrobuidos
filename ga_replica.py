# ga_replica.py

import zmq
import time
from common.config import (
    GA_REQUEST_PORT,
    GA_REPL_PUSH_PORT,
    DB_REPLICA_CONFIG
)
from common.messages import deserialize, serialize
from common.db import ejecutar_update, ejecutar_query_uno


# --- LOGICA DE NEGOCIO (Copia de ga_primary pero usando DB_REPLICA_CONFIG) ---
# Es necesaria para que la réplica pueda validar reglas cuando asuma el mando.

def manejar_prestamo(payload):
    id_usuario = payload["id_usuario"]
    codigo_libro = payload["codigo_libro"]

    # 1. Buscar el libro en la BD RÉPLICA
    row = ejecutar_query_uno(
        DB_REPLICA_CONFIG,
        "SELECT id_libro, ejemplares_disponibles FROM libros WHERE codigo = %s",
        (codigo_libro,)
    )
    if row is None:
        return {"status": "ERROR", "reason": "LIBRO_NO_EXISTE"}

    id_libro, ejemplares = row
    if ejemplares <= 0:
        return {"status": "RECHAZADO", "reason": "SIN_EJEMPLARES"}

    # 2. Crear préstamo
    ejecutar_update(
        DB_REPLICA_CONFIG,
        """
        INSERT INTO prestamos (id_usuario, id_libro, fecha_inicio, fecha_fin, renovaciones, estado)
        VALUES (%s, %s, NOW(), NOW() + INTERVAL '14 days', 0, 'ACTIVO')
        """,
        (id_usuario, id_libro)
    )

    # 3. Decrementar ejemplares
    ejecutar_update(
        DB_REPLICA_CONFIG,
        "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE id_libro = %s",
        (id_libro,)
    )
    return {"status": "OK"}


def manejar_renovacion(payload):
    id_prestamo = payload["id_prestamo"]
    row = ejecutar_query_uno(
        DB_REPLICA_CONFIG,
        "SELECT renovaciones FROM prestamos WHERE id_prestamo = %s AND estado = 'ACTIVO'",
        (id_prestamo,)
    )
    if row is None:
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    renovaciones, = row
    if renovaciones >= 2:
        return {"status": "RECHAZADO", "reason": "LIMITE_RENOVACIONES"}

    ejecutar_update(
        DB_REPLICA_CONFIG,
        """
        UPDATE prestamos
        SET renovaciones = renovaciones + 1,
            fecha_fin = fecha_fin + INTERVAL '7 days'
        WHERE id_prestamo = %s
        """,
        (id_prestamo,)
    )
    return {"status": "OK"}


def manejar_devolucion(payload):
    id_prestamo = payload["id_prestamo"]
    row = ejecutar_query_uno(
        DB_REPLICA_CONFIG,
        """
        SELECT p.id_libro FROM prestamos p
        WHERE p.id_prestamo = %s AND p.estado = 'ACTIVO'
        """,
        (id_prestamo,)
    )
    if row is None:
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    id_libro, = row
    ejecutar_update(
        DB_REPLICA_CONFIG,
        "UPDATE prestamos SET estado = 'DEVUELTO' WHERE id_prestamo = %s",
        (id_prestamo,)
    )
    ejecutar_update(
        DB_REPLICA_CONFIG,
        "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE id_libro = %s",
        (id_libro,)
    )
    return {"status": "OK"}


# --- LOGICA DE REPLICACION PASIVA (Lo que ya tenías) ---
def aplicar_replicacion(msg):
    """ Aplica ciegamente lo que manda el primario (confiamos en que él ya validó) """
    op = msg["op"]
    payload = msg["payload"]

    try:
        if op == "PRESTAMO":
            # En replicación asumimos que el INSERT y UPDATE son válidos
            # Nota: Para simplificar, aquí reusamos la lógica de negocio pero
            # en un sistema real la replicación suele ser a nivel de logs SQL, no lógica.
            # Pero dado el código actual, reusar manejar_prestamo es arriesgado si
            # el estado cambió. Para el proyecto, usaremos la lógica directa SQL
            # simplificada que tenías antes para evitar re-validar.

            codigo_libro = payload["codigo_libro"]
            row = ejecutar_query_uno(DB_REPLICA_CONFIG, "SELECT id_libro FROM libros WHERE codigo = %s",
                                     (codigo_libro,))
            if row:
                id_libro, = row
                ejecutar_update(DB_REPLICA_CONFIG,
                                "INSERT INTO prestamos (id_usuario, id_libro, fecha_inicio, fecha_fin, renovaciones, estado) VALUES (%s, %s, NOW(), NOW() + INTERVAL '14 days', 0, 'ACTIVO')",
                                (payload["id_usuario"], id_libro))
                ejecutar_update(DB_REPLICA_CONFIG,
                                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE id_libro = %s",
                                (id_libro,))

        elif op == "RENOVACION":
            ejecutar_update(DB_REPLICA_CONFIG,
                            "UPDATE prestamos SET renovaciones = renovaciones + 1, fecha_fin = fecha_fin + INTERVAL '7 days' WHERE id_prestamo = %s",
                            (payload["id_prestamo"],))

        elif op == "DEVOLUCION":
            # Necesitamos id_libro para devolver stock
            row = ejecutar_query_uno(DB_REPLICA_CONFIG, "SELECT id_libro FROM prestamos WHERE id_prestamo = %s",
                                     (payload["id_prestamo"],))
            if row:
                id_libro, = row
                ejecutar_update(DB_REPLICA_CONFIG, "UPDATE prestamos SET estado = 'DEVUELTO' WHERE id_prestamo = %s",
                                (payload["id_prestamo"],))
                ejecutar_update(DB_REPLICA_CONFIG,
                                "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE id_libro = %s",
                                (id_libro,))

        print(f"[GA-REPL] Replicación aplicada: {op}")

    except Exception as e:
        print(f"[GA-REPL] Error aplicando replicación: {e}")


# --- MAIN CON POLLER ---
def main():
    context = zmq.Context()

    # 1. Socket PULL para recibir actualizaciones del Primario (Puerto 5571)
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://*:{GA_REPL_PUSH_PORT}")

    # 2. Socket REP para actuar como Backup ante fallos (Puerto 5570)
    # Los actores intentarán conectar aquí si el primario falla.
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GA_REQUEST_PORT}")

    # Poller para escuchar ambos sockets
    poller = zmq.Poller()
    poller.register(socket_pull, zmq.POLLIN)
    poller.register(socket_rep, zmq.POLLIN)

    print(f"[GA-REPL] Escuchando REPLICACIÓN en tcp://*:{GA_REPL_PUSH_PORT}")
    print(f"[GA-REPL] Listo para FAILOVER en tcp://*:{GA_REQUEST_PORT}")

    while True:
        try:
            events = dict(poller.poll())

            # CASO A: Llega mensaje de replicación desde GA Primary
            if socket_pull in events:
                data = socket_pull.recv()
                msg = deserialize(data)
                aplicar_replicacion(msg)

            # CASO B: Llega solicitud directa de un Actor (Failover activado)
            if socket_rep in events:
                data = socket_rep.recv()
                msg = deserialize(data)
                print(f"[GA-REPL] Solicitud Failover recibida: {msg['op']}")

                # Procesar igual que el primario
                op = msg["op"]
                payload = msg["payload"]
                reply = {}

                if op == "PING":
                    reply = {"status": "ALIVE"}
                elif op == "PRESTAMO":
                    reply = manejar_prestamo(payload)
                elif op == "RENOVACION":
                    reply = manejar_renovacion(payload)
                elif op == "DEVOLUCION":
                    reply = manejar_devolucion(payload)
                else:
                    reply = {"status": "ERROR", "reason": "OP_DESCONOCIDA"}

                socket_rep.send(serialize(reply))

        except Exception as e:
            print("[GA-REPL] Error crítico:", e)
            time.sleep(1)


if __name__ == "__main__":
    main()