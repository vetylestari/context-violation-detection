# project/context_violation_detection/db/insert.py

import psycopg2
from psycopg2.extras import execute_values
from decouple import config

# Ambil koneksi dari environment
def get_connection():
    return psycopg2.connect(
        host=config("DB_HOST"),
        port=config("DB_PORT", cast=int),
        dbname=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD")
    )

# Insert banyak data sekaligus
def insert_violations(records):
    query = """
        INSERT INTO machine_learning.rns_product_violation (
            product_id, product_name, keyword, is_violation
        ) VALUES %s
        ON CONFLICT (product_id, keyword) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, query, records)
        conn.commit()
