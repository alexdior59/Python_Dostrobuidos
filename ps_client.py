# ps_client.py

import sys
import csv
import zmq
from common.config import GC_ENDPOINT
from common.messages import nuevo_mensaje, serialize, deserialize


def main():
    if len(sys.argv) < 2:
        print("Uso: python ps_client.py archivo_peticiones.csv")
        sys.exit(1)

    archivo = sys.argv[1]

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(GC_ENDPOINT)
    print(f"[PS] Conectado a GC en {GC_ENDPOINT}")

    with open(archivo, newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or row[0].startswith("#"):
                continue

            tipo = row[0].strip().upper()
            id_usuario = int(row[1]) if row[1] else None
            codigo_libro = row[2].strip() if row[2] else None
            id_prestamo = int(row[3]) if row[3] else None

            if tipo == "PRESTAMO":
                payload = {
                    "id_usuario": id_usuario,
                    "codigo_libro": codigo_libro
                }
            elif tipo in ("RENOVACION", "DEVOLUCION"):
                payload = {
                    "id_prestamo": id_prestamo
                }
            else:
                print(f"[PS] Tipo desconocido: {tipo}")
                continue

            msg = nuevo_mensaje(tipo, payload)
            socket.send(serialize(msg))
            reply_raw = socket.recv()
            reply = deserialize(reply_raw)
            print(f"[PS] Solicitud {tipo} -> Respuesta: {reply}")


if __name__ == "__main__":
    main()