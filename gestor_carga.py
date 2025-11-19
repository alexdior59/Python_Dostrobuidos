# gestor_carga.py

import zmq
from common.config import GC_ENDPOINT, GC_PUB_PORT
from common.messages import deserialize, serialize


def main():
    context = zmq.Context()

    # GC frontend: REP para PS
    socket_rep = context.socket(zmq.REP)
    socket_rep.bind(f"tcp://*:{GC_PUB_PORT - 1}")  # 5555, consistente con config.py
    print(f"[GC] Escuchando PS en {GC_ENDPOINT}")

    # GC publisher para Actores de renovación/devolución
    socket_pub = context.socket(zmq.PUB)
    socket_pub.bind(f"tcp://*:{GC_PUB_PORT}")
    print(f"[GC] Publicando a actores en tcp://*:{GC_PUB_PORT}")

    # Socket hacia Actor-Prestamo
    socket_prestamo = context.socket(zmq.REQ)
    socket_prestamo.connect("tcp://127.0.0.1:6000")
    print("[GC] Conectado a Actor-Prestamo en tcp://127.0.0.1:6000")

    while True:
        raw = socket_rep.recv()
        msg_ps = deserialize(raw)
        op = msg_ps["op"]
        payload = msg_ps["payload"]

        if op == "RENOVACION":
            # ACK inmediato al PS
            socket_rep.send(serialize({"status": "ACK", "tipo": "RENOVACION"}))
            # Publicar evento al tópico renovacion
            topic = "renovacion"
            socket_pub.send_string(topic, zmq.SNDMORE)
            socket_pub.send(serialize(msg_ps))

        elif op == "DEVOLUCION":
            socket_rep.send(serialize({"status": "ACK", "tipo": "DEVOLUCION"}))
            topic = "devolucion"
            socket_pub.send_string(topic, zmq.SNDMORE)
            socket_pub.send(serialize(msg_ps))

        elif op == "PRESTAMO":
            # Llamar sincrónicamente al Actor-Prestamo
            socket_prestamo.send(serialize(msg_ps))
            reply_actor_raw = socket_prestamo.recv()
            reply_actor = deserialize(reply_actor_raw)
            socket_rep.send(serialize(reply_actor))

        else:
            socket_rep.send(serialize({"status": "ERROR", "reason": "OP_DESCONOCIDA"}))


if __name__ == "__main__":
    main()
