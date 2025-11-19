# ga_replica.py

import zmq
from common.config import GA_REPL_PUSH_PORT, DB_REPLICA_CONFIG
from common.messages import deserialize
from common.db import ejecutar_update, ejecutar_query_uno


def aplicar_operacion_en_replica(msg):
    op = msg["op"]
    payload = msg["payload"]

    # Lógica casi igual a la de GA primario, pero sobre DB_REPLICA_CONFIG
    # Para acortar, solo replicamos el estado, asumiendo que la lógica
    # de negocio fue validada en el primario.
    if op == "PRESTAMO":
        id_usuario = payload["id_usuario"]
        codigo_libro = payload["codigo_libro"]

        row = ejecutar_query_uno(
            DB_REPLICA_CONFIG,
            "SELECT id_libro FROM libros WHERE codigo = %s",
            (codigo_libro,)
        )
        if row is None:
            return

        id_libro, = row

        ejecutar_update(
            DB_REPLICA_CONFIG,
            """
            INSERT INTO prestamos (id_usuario, id_libro, fecha_inicio, fecha_fin, renovaciones, estado)
            VALUES (%s, %s, NOW(), NOW() + INTERVAL '14 days', 0, 'ACTIVO')
            """,
            (id_usuario, id_libro)
        )
        ejecutar_update(
            DB_REPLICA_CONFIG,
            "UPDATE libros SET ejemplares_disponibles = ejemplares_disponibles - 1 WHERE id_libro = %s",
            (id_libro,)
        )

    elif op == "RENOVACION":
        id_prestamo = payload["id_prestamo"]
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

    elif op == "DEVOLUCION":
        id_prestamo = payload["id_prestamo"]
        row = ejecutar_query_uno(
            DB_REPLICA_CONFIG,
            "SELECT id_libro FROM prestamos WHERE id_prestamo = %s",
            (id_prestamo,)
        )
        if row is None:
            return
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


def main():
    context = zmq.Context()
    socket_pull = context.socket(zmq.PULL)
    socket_pull.bind(f"tcp://*:{GA_REPL_PUSH_PORT}")

    print(f"[GA-REPL] Escuchando réplica en tcp://*:{GA_REPL_PUSH_PORT}")

    while True:
        data = socket_pull.recv()
        msg = deserialize(data)
        aplicar_operacion_en_replica(msg)


if __name__ == "__main__":
    main()
