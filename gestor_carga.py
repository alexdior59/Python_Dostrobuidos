# gestor_carga.py

import zmq
from common.config import GC_ENDPOINT, GC_PUB_PORT, ACTOR_PRESTAMO_ENDPOINT
from common.messages import deserialize, serialize


def main():
    context = zmq.Context()

    # 1. Socket REP para recibir peticiones de los Clientes (PS)
    # Usamos la lógica original: puerto 5555
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GC_PUB_PORT - 1}")
    print(f"[GC] Escuchando PS en {GC_ENDPOINT}")

    # 2. Socket PUB para notificar a Actores (Renovación/Devolución)
    socket_pub = context.socket(zmq.PUB)
    socket_pub.bind(f"tcp://*:{GC_PUB_PORT}")
    print(f"[GC] Publicando a actores en tcp://*:{GC_PUB_PORT}")

    # 3. Socket REQ para hablar con el Actor Préstamo (Síncrono)
    # IMPORTANTE: Usa la variable de config, no una IP fija.
    socket_prestamo = context.socket(zmq.REQ)
    socket_prestamo.connect(ACTOR_PRESTAMO_ENDPOINT)
    print(f"[GC] Conectado a Actor-Prestamo en {ACTOR_PRESTAMO_ENDPOINT}")

    while True:
        try:
            # Recibir mensaje del cliente
            raw = socket_rep.recv()
            msg_ps = deserialize(raw)
            op = msg_ps["op"]

            print(f"[GC] Procesando: {op}")

            if op == "RENOVACION":
                # 1. Responder OK al cliente inmediatamente (Síncrono)
                socket_rep.send(serialize({"status": "ACK", "tipo": "RENOVACION"}))
                # 2. Avisar al Actor para que procese (Asíncrono)
                topic = "renovacion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "DEVOLUCION":
                # 1. Responder OK al cliente inmediatamente
                socket_rep.send(serialize({"status": "ACK", "tipo": "DEVOLUCION"}))
                # 2. Avisar al Actor
                topic = "devolucion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "PRESTAMO":
                # Comunicación Síncrona Total: GC espera al Actor
                socket_prestamo.send(serialize(msg_ps))

                # Esperar respuesta del Actor (que a su vez esperó al GA/JSON)
                reply_actor_raw = socket_prestamo.recv()
                reply_actor = deserialize(reply_actor_raw)

                # Responder al cliente con el resultado final
                socket_rep.send(serialize(reply_actor))

            else:
                socket_rep.send(serialize({"status": "ERROR", "reason": "OP_DESCONOCIDA"}))

        except Exception as e:
            print(f"[GC] Error: {e}")
            # Intentar revivir el socket o responder error si es posible
            try:
                socket_rep.send(serialize({"status": "ERROR", "reason": "INTERNAL_ERROR"}))
            except:
                pass


if __name__ == "__main__":
    main()