# common/messages.py

import json
import uuid
from datetime import datetime


def nuevo_mensaje(op, payload):
    return {
        "id": str(uuid.uuid4()),
        "op": op,
        "timestamp": datetime.utcnow().isoformat(),
        "payload": payload
    }


def serialize(msg: dict) -> bytes:
    return json.dumps(msg).encode("utf-8")


def deserialize(data: bytes) -> dict:
    return json.loads(data.decode("utf-8"))