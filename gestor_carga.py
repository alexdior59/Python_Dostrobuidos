# gestor_carga.py

import zmq
from common.config import GC_ENDPOINT, GC_PUB_PORT, ACTOR_PRESTAMO_ENDPOINT
from common.messages import deserialize, serialize


def main():
    context = zmq.Context()

    # GC frontend: REP para PS
    socket_rep = context.socket(zmq.REP)
    # Usamos el puerto -1 según tu lógica original, o mejor usar GC_FRONTEND_PORT directo
    # Aquí mantengo tu lógica original de config.py: GC_PUB_PORT - 1 = 5555
    socket_rep.bind(f"tcp://*:{GC_PUB_PORT - 1}")
    print(f"[GC] Escuchando PS en {GC_ENDPOINT}")

    # GC publisher para Actores de renovación/devolución
    socket_pub = context.socket(zmq.PUB)
    socket_pub.bind(f"tcp://*:{GC_PUB_PORT}")
    print(f"[GC] Publicando a actores en tcp://*:{GC_PUB_PORT}")

    # Socket hacia Actor-Prestamo
    # CORRECCION: Usa la variable de config, no hardcodeado
    socket_prestamo = context.socket(zmq.REQ)
    socket_prestamo.connect(ACTOR_PRESTAMO_ENDPOINT)
    print(f"[GC] Conectado a Actor-Prestamo en {ACTOR_PRESTAMO_ENDPOINT}")

    while True:
        try:
            raw = socket_rep.recv()
            msg_ps = deserialize(raw)
            op = msg_ps["op"]

            # Log simple para ver actividad
            print(f"[GC] Recibido: {op}")

            if op == "RENOVACION":
                # ACK inmediato al PS
                socket_rep.send(serialize({"status": "ACK", "tipo": "RENOVACION"}))
                # Publicar evento
                topic = "renovacion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "DEVOLUCION":
                socket_rep.send(serialize({"status": "ACK", "tipo": "DEVOLUCION"}))
                topic = "devolucion"
                socket_pub.send_string(topic, zmq.SNDMORE)
                socket_pub.send(serialize(msg_ps))

            elif op == "PRESTAMO":
                # Llamada síncrona al Actor
                socket_prestamo.send(serialize(msg_ps))
                reply_actor_raw = socket_prestamo.recv()
                reply_actor = deserialize(reply_actor_raw)
                socket_rep.send(serialize(reply_actor))

            else:
                socket_rep.send(serialize({"status": "ERROR", "reason": "OP_DESCONOCIDA"}))

        except Exception as e:
            print(f"[GC] Error procesando solicitud: {e}")
            # En caso de error fatal en el loop, intentamos enviar error al cliente si socket sigue vivo
            try:
                socket_rep.send(serialize({"status": "ERROR", "reason": "INTERNAL_ERROR"}))
            except:
                pass


if __name__ == "__main__":
    main()