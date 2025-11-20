# test_rendimiento.py

import time
import threading
import zmq
import statistics
import random
from common.config import GC_ENDPOINT
from common.messages import nuevo_mensaje, serialize, deserialize

# Parámetros de prueba
NUM_CLIENTES = 10
DURACION_PRUEBA = 120
TIEMPO_ENTRE_REQ = 0.1

tiempos_respuesta = []
total_solicitudes = 0
lock = threading.Lock()
stop_event = threading.Event()


def cliente_simulado(id_cliente):
    global total_solicitudes
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(GC_ENDPOINT)

    libros_prueba = ["ISBN-001", "ISBN-002", "ISBN-003"]

    while not stop_event.is_set():
        try:
            op_tipo = random.choice(["PRESTAMO", "DEVOLUCION"])
            payload = {}

            if op_tipo == "PRESTAMO":
                payload = {
                    "id_usuario": random.randint(100, 999),
                    "codigo_libro": random.choice(libros_prueba)
                }
            else:
                payload = {"id_prestamo": random.randint(1, 50)}

            msg = nuevo_mensaje(op_tipo, payload)

            start = time.time()
            socket.send(serialize(msg))
            socket.recv()
            end = time.time()

            duracion = end - start

            with lock:
                tiempos_respuesta.append(duracion)
                total_solicitudes += 1

            time.sleep(TIEMPO_ENTRE_REQ)

        except Exception as e:
            print(f"Error en cliente {id_cliente}: {e}")
            break

    socket.close()
    context.term()


def main():
    print(f"--- INICIANDO PRUEBA DE RENDIMIENTO ---")
    print(f"Clientes simultáneos: {NUM_CLIENTES}")
    print(f"Duración: {DURACION_PRUEBA} segundos")

    threads = []

    for i in range(NUM_CLIENTES):
        t = threading.Thread(target=cliente_simulado, args=(i,))
        t.start()
        threads.append(t)

    time.sleep(DURACION_PRUEBA)

    stop_event.set()
    for t in threads:
        t.join()

    if tiempos_respuesta:
        promedio = statistics.mean(tiempos_respuesta)
        desviacion = statistics.stdev(tiempos_respuesta) if len(tiempos_respuesta) > 1 else 0
        throughput = total_solicitudes / DURACION_PRUEBA

        print("\n" + "=" * 40)
        print("RESULTADOS")
        print("=" * 40)
        print(f"Total Solicitudes:    {total_solicitudes}")
        print(f"Throughput (TPS):     {throughput:.2f} req/seg")
        print(f"Tiempo Promedio:      {promedio:.4f} seg")
        print(f"Desviación Estándar:  {desviacion:.4f} seg")
        print("=" * 40)
    else:
        print("No se completaron solicitudes.")


if __name__ == "__main__":
    main()