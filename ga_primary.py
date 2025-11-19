# ga_primary.py

import zmq
import time
from common.config import (
    GA_REQUEST_PORT, GA_REPL_PUSH_ENDPOINT,
    DB_PRIMARY_CONFIG
)
from common import config
from common.messages import deserialize, serialize
from common.db import ejecutar_update, ejecutar_query_uno


# In-memory idempotency (simple demo)
PROCESSED_MESSAGES = set()


def manejar_prestamo(payload):
    id_usuario = payload["id_usuario"]
    codigo_libro = payload["codigo_libro"]

    # 1. Buscar el libro
    row = ejecutar_query_uno(
        DB_PRIMARY_CONFIG,
        "SELECT id_libro, ejemplares_disponibles FROM libros WHERE codigo = %s",
        (codigo_libro,)
    )
    if row is None:
        return {"status": "ERROR", "reason": "LIBRO_NO_EXISTE"}

    id_libro, ejemplares = row
    if ejemplares <= 0:
        return {"status": "RECHAZADO", "reason": "SIN_EJEMPLARES"}

    # 2. Crear préstamo (2 semanas)
    ejecutar_update(
        DB_PRIMARY_CONFIG,
        """
        INSERT INTO prestamos (id_usuario, id_libro, fecha_inicio, fecha_fin, renovaciones, estado)
        VALUES (%s, %s, NOW(), NOW() + INTERVAL '14 days', 0, 'ACTIVO')
        """,
        (id_usuario, id_libro)
    )

    # 3. Decrementar ejemplares
    ejecutar_update(
        DB_PRIMARY_CONFIG,
        "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE id_libro = %s",
        (id_libro,)
    )

    return {"status": "OK"}


def manejar_renovacion(payload):
    id_prestamo = payload["id_prestamo"]

    row = ejecutar_query_uno(
        DB_PRIMARY_CONFIG,
        "SELECT renovaciones FROM prestamos WHERE id_prestamo = %s AND estado = 'ACTIVO'",
        (id_prestamo,)
    )
    if row is None:
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    renovaciones, = row
    if renovaciones >= 2:
        return {"status": "RECHAZADO", "reason": "LIMITE_RENOVACIONES"}

    # Actualizar fecha_fin y contador de renovaciones
    ejecutar_update(
        DB_PRIMARY_CONFIG,
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
        DB_PRIMARY_CONFIG,
        """
        SELECT p.id_libro
        FROM prestamos p
        WHERE p.id_prestamo = %s AND p.estado = 'ACTIVO'
        """,
        (id_prestamo,)
    )
    if row is None:
        # Caso devolución inexistente (se puede registrar en tabla aparte si quieres)
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    id_libro, = row

    # 1. Marcar préstamo como devuelto
    ejecutar_update(
        DB_PRIMARY_CONFIG,
        "UPDATE prestamos SET estado = 'DEVUELTO' WHERE id_prestamo = %s",
        (id_prestamo,)
    )

    # 2. Incrementar ejemplares
    ejecutar_update(
        DB_PRIMARY_CONFIG,
        "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles + 1 WHERE id_libro = %s",
        (id_libro,)
    )

    return {"status": "OK"}


def aplicar_operacion(msg):
    op = msg["op"]
    payload = msg["payload"]

    if op == "PING":
        return {"status": "ALIVE"}

    if op == "PRESTAMO":
        return manejar_prestamo(payload)
    elif op == "RENOVACION":
        return manejar_renovacion(payload)
    elif op == "DEVOLUCION":
        return manejar_devolucion(payload)
    else:
        return {"status": "ERROR", "reason": "OP_DESCONOCIDA"}


def main():
    context = zmq.Context()

    # REP para solicitudes de Actores
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GA_REQUEST_PORT}")

    # PUSH para replicar operaciones a GA réplica
    socket_push = context.socket(zmq.PUSH)
    socket_push.connect(GA_REPL_PUSH_ENDPOINT)

    print(f"[GA-PRIMARY] Escuchando en tcp://*:{GA_REQUEST_PORT}")
    print(f"[GA-PRIMARY] Replicando hacia {GA_REPL_PUSH_ENDPOINT}")

    while True:
        try:
            data = socket_rep.recv()
            msg = deserialize(data)
            msg_id = msg["id"]

            if msg_id in PROCESSED_MESSAGES:
                # Idempotencia simple
                reply = {"status": "OK", "info": "DUPLICADO_IGNORADO"}
            else:
                result = aplicar_operacion(msg)
                if result.get("status") in ("OK", "RECHAZADO", "ERROR"):
                    PROCESSED_MESSAGES.add(msg_id)
                reply = result

                # Replicar sólo operaciones de negocio, no PING
                if msg["op"] != "PING":
                    socket_push.send(data)

            socket_rep.send(serialize(reply))

        except Exception as e:
            print("[GA-PRIMARY] Error:", e)
            time.sleep(1)


if __name__ == "__main__":
    main()
