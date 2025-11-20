# ga_replica.py

import zmq
import time
from common.config import (
    GA_REQUEST_PORT,
    GA_REPL_PUSH_PORT,
    GA_REPLICA_REQ_PORT,
    DB_REPLICA_PATH
)
from common.messages import deserialize, serialize
from common.db import (
    inicializar_db, buscar_libro_por_codigo,
    crear_prestamo, buscar_prestamo_por_id,
    renovar_prestamo_db, devolver_prestamo_db
)


# Funciones de negocio para modo Failover

def manejar_prestamo_repl(payload):
    libro = buscar_libro_por_codigo(DB_REPLICA_PATH, payload["codigo_libro"])
    if not libro: return {"status": "ERROR", "reason": "LIBRO_NO_EXISTE"}
    if libro["ejemplares"] <= 0: return {"status": "RECHAZADO", "reason": "SIN_EJEMPLARES"}
    crear_prestamo(DB_REPLICA_PATH, payload["id_usuario"], libro)
    return {"status": "OK"}


def manejar_renovacion_repl(payload):
    p = buscar_prestamo_por_id(DB_REPLICA_PATH, payload["id_prestamo"])
    if not p: return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}
    if p["renovaciones"] >= 2: return {"status": "RECHAZADO", "reason": "LIMITE_RENOVACIONES"}
    renovar_prestamo_db(DB_REPLICA_PATH, p)
    return {"status": "OK"}


def manejar_devolucion_repl(payload):
    p = buscar_prestamo_por_id(DB_REPLICA_PATH, payload["id_prestamo"])
    if not p: return {"status": "ERROR", "reason": "PRESTAMO_NO_ACTIVO"}
    devolver_prestamo_db(DB_REPLICA_PATH, p)
    return {"status": "OK"}


# Procesamiento de replicación

def aplicar_replicacion(msg):
    op = msg["op"]
    payload = msg["payload"]
    try:
        if op == "PRESTAMO":
            libro = buscar_libro_por_codigo(DB_REPLICA_PATH, payload["codigo_libro"])
            if libro: crear_prestamo(DB_REPLICA_PATH, payload["id_usuario"], libro)

        elif op == "RENOVACION":
            p = buscar_prestamo_por_id(DB_REPLICA_PATH, payload["id_prestamo"])
            if p: renovar_prestamo_db(DB_REPLICA_PATH, p)

        elif op == "DEVOLUCION":
            p = buscar_prestamo_por_id(DB_REPLICA_PATH, payload["id_prestamo"])
            if p: devolver_prestamo_db(DB_REPLICA_PATH, p)

        print(f"[GA-REPL] Evento replicado: {op}")
    except Exception as e:
        print(f"[GA-REPL] Error en replicación: {e}")


def main():
    inicializar_db(DB_REPLICA_PATH)
    context = zmq.Context()

    # Socket PULL para stream de replicación
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://*:{GA_REPL_PUSH_PORT}")

    # Socket REP para peticiones directas (Modo Failover)
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GA_REPLICA_REQ_PORT}")

    poller = zmq.Poller()
    poller.register(socket_pull, zmq.POLLIN)
    poller.register(socket_rep, zmq.POLLIN)

    print(f"[GA-REPL] Servicio Réplica activo. Failover en puerto {GA_REPLICA_REQ_PORT}")

    while True:
        try:
            events = dict(poller.poll())

            if socket_pull in events:
                msg = deserialize(socket_pull.recv())
                aplicar_replicacion(msg)

            if socket_rep in events:
                # Atención de solicitud por fallo en primario
                data = socket_rep.recv()
                msg = deserialize(data)
                print(f"[GA-REPL] Solicitud Failover recibida: {msg['op']}")

                op = msg["op"]
                if op == "PING":
                    reply = {"status": "ALIVE"}
                elif op == "PRESTAMO":
                    reply = manejar_prestamo_repl(msg["payload"])
                elif op == "RENOVACION":
                    reply = manejar_renovacion_repl(msg["payload"])
                elif op == "DEVOLUCION":
                    reply = manejar_devolucion_repl(msg["payload"])
                else:
                    reply = {"status": "ERROR"}

                socket_rep.send(serialize(reply))

        except Exception as e:
            print("[GA-REPL] Error:", e)


if __name__ == "__main__":
    main()