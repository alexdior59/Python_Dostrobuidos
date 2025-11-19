# common/db.py

import psycopg2
from contextlib import contextmanager


@contextmanager
def get_connection(db_config):
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"],
    )
    try:
        yield conn
    finally:
        conn.close()


def ejecutar_update(db_config, query, params=None):
    if params is None:
        params = ()
    with get_connection(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()


def ejecutar_query_uno(db_config, query, params=None):
    if params is None:
        params = ()
    with get_connection(db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchone()
