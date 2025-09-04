# project/context_violation_detection/db/insert.py

import psycopg2
from psycopg2.extras import execute_values
from decouple import config
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        host=config("DB_HOST"),
        port=config("DB_PORT", cast=int),
        dbname=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD")
    )

def insert_violations(results: list[tuple]):
    from datetime import datetime

    # Debug 1: cek input results
    print(f"[DEBUG] Jumlah results masuk: {len(results)}")
    if len(results) > 0:
        print(f"[DEBUG] Sample result[0]: {results[0]}")

    records = [r + (datetime.utcnow(),) for r in results]

    # Debug 2: cek hasil records yang mau diinsert
    print(f"[DEBUG] Jumlah records final: {len(records)}")
    if len(records) > 0:
        print(f"[DEBUG] Sample record[0]: {records[0]}")

    query = """
        INSERT INTO machine_learning.rns_product_violation (
            product_id, product_name, keyword, is_violation, date_in
        ) VALUES %s
        ON CONFLICT (product_id, keyword) DO NOTHING
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, query, records)
            print(f"[DEBUG] Status after execute_values, rowcount = {cur.rowcount}")
        conn.commit()