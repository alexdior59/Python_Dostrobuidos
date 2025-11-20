# common/db.py
import json
import os
from datetime import datetime, timedelta


def inicializar_db(filepath):
    """Crea el archivo JSON con datos semilla si no existe."""
    if not os.path.exists(filepath):
        datos_iniciales = {
            "libros": [
                {"id": 1, "codigo": "ISBN-001", "titulo": "Sistemas Distribuidos", "ejemplares": 5},
                {"id": 2, "codigo": "ISBN-002", "titulo": "Redes de Computadoras", "ejemplares": 3},
                {"id": 3, "codigo": "ISBN-003", "titulo": "Ingeniería de Software", "ejemplares": 10}
            ],
            "prestamos": []
        }
        guardar_json(filepath, datos_iniciales)
        print(f"[DB] Base de datos creada en {filepath}")


def leer_json(filepath):
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        inicializar_db(filepath)
        return leer_json(filepath)


def guardar_json(filepath, data):
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=4, default=str)


# --- Funciones Helper para emular SQL ---

def buscar_libro_por_codigo(filepath, codigo):
    db = leer_json(filepath)
    for libro in db["libros"]:
        if libro["codigo"] == codigo:
            return libro
    return None


def buscar_prestamo_por_id(filepath, id_prestamo):
    db = leer_json(filepath)
    for p in db["prestamos"]:
        if p["id_prestamo"] == id_prestamo and p["estado"] == "ACTIVO":
            return p
    return None


def crear_prestamo(filepath, id_usuario, libro_obj):
    db = leer_json(filepath)

    # Actualizar stock libro
    for l in db["libros"]:
        if l["id"] == libro_obj["id"]:
            l["ejemplares"] -= 1
            break

    # Crear registro préstamo
    nuevo_id = len(db["prestamos"]) + 1
    nuevo_prestamo = {
        "id_prestamo": nuevo_id,
        "id_usuario": id_usuario,
        "id_libro": libro_obj["id"],
        "fecha_inicio": datetime.now().isoformat(),
        "fecha_fin": (datetime.now() + timedelta(days=14)).isoformat(),
        "renovaciones": 0,
        "estado": "ACTIVO"
    }
    db["prestamos"].append(nuevo_prestamo)
    guardar_json(filepath, db)
    return nuevo_id


def renovar_prestamo_db(filepath, prestamo_obj):
    db = leer_json(filepath)
    for p in db["prestamos"]:
        if p["id_prestamo"] == prestamo_obj["id_prestamo"]:
            p["renovaciones"] += 1
            # Parsear fecha para sumar días
            fecha_fin_dt = datetime.fromisoformat(p["fecha_fin"])
            p["fecha_fin"] = (fecha_fin_dt + timedelta(days=7)).isoformat()
            break
    guardar_json(filepath, db)


def devolver_prestamo_db(filepath, prestamo_obj):
    db = leer_json(filepath)
    id_libro = prestamo_obj["id_libro"]

    # Marcar préstamo devuelto
    for p in db["prestamos"]:
        if p["id_prestamo"] == prestamo_obj["id_prestamo"]:
            p["estado"] = "DEVUELTO"
            break

    # Devolver stock
    for l in db["libros"]:
        if l["id"] == id_libro:
            l["ejemplares"] += 1
            break

    guardar_json(filepath, db)