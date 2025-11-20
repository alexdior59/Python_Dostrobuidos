# ga_primary.py

import zmq
import time
from common.config import (
    GA_REQUEST_PORT, GA_REPL_PUSH_ENDPOINT,
    DB_PRIMARY_PATH
)
from common.messages import deserialize, serialize
from common.db import (
    inicializar_db, buscar_libro_por_codigo,
    crear_prestamo, buscar_prestamo_por_id,
    renovar_prestamo_db, devolver_prestamo_db
)

PROCESSED_MESSAGES = set()


def manejar_prestamo(payload):
    id_usuario = payload["id_usuario"]
    codigo_libro = payload["codigo_libro"]

    libro = buscar_libro_por_codigo(DB_PRIMARY_PATH, codigo_libro)

    if libro is None:
        return {"status": "ERROR", "reason": "LIBRO_NO_EXISTE"}

    if libro["ejemplares"] <= 0:
        return {"status": "RECHAZADO", "reason": "SIN_EJEMPLARES"}

    crear_prestamo(DB_PRIMARY_PATH, id_usuario, libro)
    return {"status": "OK"}


def manejar_renovacion(payload):
    id_prestamo = payload["id_prestamo"]

    prestamo = buscar_prestamo_por_id(DB_PRIMARY_PATH, id_prestamo)

    if prestamo is None:
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    if prestamo["renovaciones"] >= 2:
        return {"status": "RECHAZADO", "reason": "LIMITE_RENOVACIONES"}

    renovar_prestamo_db(DB_PRIMARY_PATH, prestamo)
    return {"status": "OK"}


def manejar_devolucion(payload):
    id_prestamo = payload["id_prestamo"]

    prestamo = buscar_prestamo_por_id(DB_PRIMARY_PATH, id_prestamo)

    if prestamo is None:
        return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}

    devolver_prestamo_db(DB_PRIMARY_PATH, prestamo)
    return {"status": "OK"}


def aplicar_operacion(msg):
    op = msg["op"]
    payload = msg["payload"]

    if op == "PING":
        return {"status": "ALIVE"}
    elif op == "PRESTAMO":
        return manejar_prestamo(payload)
    elif op == "RENOVACION":
        return manejar_renovacion(payload)
    elif op == "DEVOLUCION":
        return manejar_devolucion(payload)
    else:
        return {"status": "ERROR", "reason": "OP_DESCONOCIDA"}


def main():
    inicializar_db(DB_PRIMARY_PATH)

    context = zmq.Context()
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GA_REQUEST_PORT}")

    socket_push = context.socket(zmq.PUSH)
    socket_push.connect(GA_REPL_PUSH_ENDPOINT)

    print(f"[GA-PRIMARY] Servicio iniciado en tcp://*:{GA_REQUEST_PORT}")

    while True:
        try:
            data = socket_rep.recv()
            msg = deserialize(data)
            msg_id = msg["id"]

            if msg_id in PROCESSED_MESSAGES:
                reply = {"status": "OK", "info": "DUPLICADO_IGNORADO"}
            else:
                result = aplicar_operacion(msg)
                reply = result
                if result.get("status") in ("OK", "RECHAZADO", "ERROR"):
                    PROCESSED_MESSAGES.add(msg_id)
                    # Replicación de eventos de negocio
                    if msg["op"] != "PING":
                        socket_push.send(data)

            socket_rep.send(serialize(reply))

        except Exception as e:
            print("[GA-PRIMARY] Error:", e)
            time.sleep(1)


if __name__ == "__main__":
    main()