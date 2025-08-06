from typing import List
import psycopg2
import pandas as pd
from decouple import config


def get_connection():
    return psycopg2.connect(
        host=config("DB_HOST"),
        port=config("DB_PORT", cast=int),
        dbname=config("DB_NAME"),
        user=config("DB_USER"),
        password=config("DB_PASSWORD"),
    )
def fetch_forbidden_words() -> List[str]:
    query = """
        SELECT keyword 
        FROM rns_forbidden_word;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [row[0] for row in rows]

# def fetch_forbidden_words(context_id: int = 2) -> list[str]:
#     query = """
#         SELECT keyword 
#         FROM rns_forbidden_word f
#         JOIN rns_forbidden_word_context fwc ON f.forbidden_word_id = fwc.forbidden_word_id
#         WHERE fwc.context_id = %s;
#     """
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute(query, (context_id,))
#             rows = cur.fetchall()
#             return [row[0] for row in rows]


def fetch_products() -> pd.DataFrame:
    query = """
        SELECT product_id, product_name 
        FROM rns_product
        WHERE status_record != 'D'
        LIMIT 1000;
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


def save_to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)