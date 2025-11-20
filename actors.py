# actors.py

import zmq
import time
from common.config import (
    GC_PUB_ENDPOINT,
    GA_PRIMARY_ENDPOINT,
    GA_REPLICA_ENDPOINT
)
from common.messages import deserialize, serialize, nuevo_mensaje


def enviar_ga_con_failover(context, msg_dict, timeout_ms=2000):
    for endpoint in (GA_PRIMARY_ENDPOINT, GA_REPLICA_ENDPOINT):
        socket = context.socket(zmq.REQ)
        socket.connect(endpoint)
        socket.RCVTIMEO = timeout_ms
        socket.SNDTIMEO = timeout_ms

        try:
            socket.send(serialize(msg_dict))
            reply_raw = socket.recv()
            reply = deserialize(reply_raw)
            socket.close()
            return reply
        except zmq.error.Again:
            print(f"[ACTOR] Timeout en {endpoint}, intentando siguiente nodo...")
            socket.close()
            continue
        except Exception as e:
            print(f"[ACTOR] Error de conexión {endpoint}: {e}")
            socket.close()
            continue

    return {"status": "ERROR", "reason": "GA_NO_DISPONIBLE"}


def actor_prestamo():
    context = zmq.Context()
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind("tcp://*:6000")
    print("[Actor-Prestamo] Escuchando en tcp://*:6000")

    while True:
        try:
            data = socket_rep.recv()
            msg_gc = deserialize(data)
            payload = msg_gc["payload"]

            print(f"[Actor-Prestamo] Procesando solicitud...")

            msg_ga = nuevo_mensaje("PRESTAMO", payload)
            reply_ga = enviar_ga_con_failover(context, msg_ga)

            socket_rep.send(serialize(reply_ga))
        except Exception as e:
            print(f"[Actor-Prestamo] Error: {e}")


def actor_subscriptor(topic):
    context = zmq.Context()

    sub = context.socket(zmq.SUB)
    sub.connect(GC_PUB_ENDPOINT)
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    print(f"[Actor-{topic}] Suscrito a '{topic}' en {GC_PUB_ENDPOINT}")

    while True:
        try:
            # Manejo de mensajes multipart (Tópico + Payload)
            frames = sub.recv_multipart()

            if len(frames) == 2:
                _, msg_raw = frames
                msg_gc = deserialize(msg_raw)

                op = "RENOVACION" if topic == "renovacion" else "DEVOLUCION"
                msg_ga = nuevo_mensaje(op, msg_gc["payload"])

                print(f"[Actor-{topic}] Enviando operación a GA...")
                reply_ga = enviar_ga_con_failover(context, msg_ga)
                print(f"[Actor-{topic}] Respuesta GA: {reply_ga}")
            else:
                print(f"[Actor-{topic}] Formato de mensaje inválido.")

        except Exception as e:
            print(f"[Actor-{topic}] Excepción: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python actors.py [prestamo|renovacion|devolucion]")
        exit(1)

    modo = sys.argv[1]
    if modo == "prestamo":
        actor_prestamo()
    elif modo == "renovacion":
        actor_subscriptor("renovacion")
    elif modo == "devolucion":
        actor_subscriptor("devolucion")
    else:
        print("Modo desconocido")