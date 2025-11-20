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
    """
    Envía una operación al GA. Si el primario no responde, intenta con la réplica.
    """
    # Intentamos primero con el endpoint del primario, luego con el de la réplica
    for endpoint in (GA_PRIMARY_ENDPOINT, GA_REPLICA_ENDPOINT):
        socket = context.socket(zmq.REQ)
        socket.connect(endpoint)
        socket.RCVTIMEO = timeout_ms
        socket.SNDTIMEO = timeout_ms

        try:
            # print(f"[ACTOR] Enviando a {endpoint}...") # Debug opcional
            socket.send(serialize(msg_dict))
            reply_raw = socket.recv()
            reply = deserialize(reply_raw)
            socket.close()
            return reply
        except zmq.error.Again:
            print(f"[ACTOR] Timeout: GA no respondió en {endpoint}, probando siguiente...")
            socket.close()
            continue
        except Exception as e:
            print(f"[ACTOR] Error conectando a {endpoint}: {e}")
            socket.close()
            continue

    return {"status": "ERROR", "reason": "GA_NO_DISPONIBLE"}


def actor_prestamo():
    """
    Actor que atiende solicitudes de préstamo de forma síncrona desde el GC.
    Escucha en tcp://*:6000.
    """
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

            # Empaquetamos mensaje para GA
            msg_ga = nuevo_mensaje("PRESTAMO", payload)
            reply_ga = enviar_ga_con_failover(context, msg_ga)

            socket_rep.send(serialize(reply_ga))
        except Exception as e:
            print(f"[Actor-Prestamo] Error: {e}")


def actor_subscriptor(topic):
    """
    Actor genérico que se suscribe a un tópico del GC (renovacion/devolucion).
    CORREGIDO: Ahora usa recv_multipart para leer mensajes divididos.
    """
    context = zmq.Context()

    sub = context.socket(zmq.SUB)
    sub.connect(GC_PUB_ENDPOINT)
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)
    print(f"[Actor-{topic}] Suscrito a topic '{topic}' en {GC_PUB_ENDPOINT}")

    while True:
        try:
            # --- CORRECCION AQUI ---
            # El GC envía [Topico, Mensaje], así que usamos recv_multipart
            frames = sub.recv_multipart()

            if len(frames) == 2:
                topic_bytes, msg_raw = frames
                # topic_str = topic_bytes.decode('utf-8') # No lo necesitamos realmente
                msg_gc = deserialize(msg_raw)  # Deserializamos la segunda parte (JSON)

                op = "RENOVACION" if topic == "renovacion" else "DEVOLUCION"
                msg_ga = nuevo_mensaje(op, msg_gc["payload"])

                print(f"[Actor-{topic}] Reenviando operación a GA...")
                reply_ga = enviar_ga_con_failover(context, msg_ga)
                print(f"[Actor-{topic}] Resultado GA: {reply_ga}")
            else:
                print(f"[Actor-{topic}] Error: Formato de mensaje incorrecto recibido.")

        except Exception as e:
            print(f"[Actor-{topic}] Error procesando mensaje: {e}")


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