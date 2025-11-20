# gestor_carga.py

import zmq
from common.config import GC_ENDPOINT, GC_PUB_PORT, ACTOR_PRESTAMO_ENDPOINT
from common.messages import deserialize, serialize


def main():
    context = zmq.Context()

    # Socket REP para recibir peticiones de clientes (PS)
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GC_PUB_PORT - 1}")
    print(f"[GC] Escuchando clientes en {GC_ENDPOINT}")

    # Socket PUB para distribución de eventos (Renovación/Devolución)
    socket_pub = context.socket(zmq.PUB)
    socket_pub.bind(f"tcp://*:{GC_PUB_PORT}")
    print(f"[GC] Publicando eventos en tcp://*:{GC_PUB_PORT}")

    # Socket REQ para comunicación síncrona con Actor Préstamo
    socket_prestamo = context.socket(zmq.REQ)
    socket_prestamo.connect(ACTOR_PRESTAMO_ENDPOINT)
    print(f"[GC] Conectado a Actor-Prestamo en {ACTOR_PRESTAMO_ENDPOINT}")

    while True:
        try:
            raw = socket_rep.recv()
            msg_ps = deserialize(raw)
            op = msg_ps["op"]

            print(f"[GC] Procesando: {op}")

            if op == "RENOVACION":
                socket_rep.send(serialize({"status": "ACK", "tipo": "RENOVACION"}))

                topic = "renovacion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "DEVOLUCION":
                socket_rep.send(serialize({"status": "ACK", "tipo": "DEVOLUCION"}))

                topic = "devolucion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "PRESTAMO":
                # Delegación síncrona al actor
                socket_prestamo.send(serialize(msg_ps))

                reply_actor_raw = socket_prestamo.recv()
                reply_actor = deserialize(reply_actor_raw)

                socket_rep.send(serialize(reply_actor))

            else:
                socket_rep.send(serialize({"status": "ERROR", "reason": "OP_DESCONOCIDA"}))

        except Exception as e:
            print(f"[GC] Error: {e}")
            try:
                socket_rep.send(serialize({"status": "ERROR", "reason": "INTERNAL_ERROR"}))
            except:
                pass


if __name__ == "__main__":
    main()