from pathlib import Path
from typing import List
import pandas as pd
from decouple import config
from sqlalchemy import create_engine, text


def get_connection():
    return create_engine(
        f"postgresql+psycopg2://{config('DB_USER')}:{config('DB_PASSWORD')}"
        f"@{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_NAME')}"
    )


def fetch_forbidden_words() -> List[str]:
    query = text("""
        SELECT keyword 
        FROM public.rns_forbidden_word
        WHERE keyword_type = 'product_name';
    """)
    engine = get_connection()
    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]


def fetch_products() -> pd.DataFrame:
    query = """
        SELECT product_id, product_name
        FROM rns_product
        WHERE status_record != 'D';
    """
    engine = get_connection()
    return pd.read_sql_query(query, engine)
    
DATA_DIR = Path("data/parquet")
PRODUCTS_PARQUET = DATA_DIR / "products.parquet"

def fetch_products_range(start_id: int, end_id: int) -> pd.DataFrame:
    if not PRODUCTS_PARQUET.exists():
        raise FileNotFoundError(f"{PRODUCTS_PARQUET} not found. Run prepare_data first.")

    df = pd.read_parquet(PRODUCTS_PARQUET)
    return df[(df["product_id"] >= start_id) & (df["product_id"] <= end_id)]


def save_to_parquet(df: pd.DataFrame, path: str):
    df.to_parquet(path, index=False)

# from typing import List
# import psycopg2
# import pandas as pd
# from decouple import config


# def get_connection():
#     return psycopg2.connect(
#         host=config("DB_HOST"),
#         port=config("DB_PORT", cast=int),
#         dbname=config("DB_NAME"),
#         user=config("DB_USER"),
#         password=config("DB_PASSWORD"),
#     )
# def fetch_forbidden_words() -> List[str]:
#     query = """
#         SELECT keyword 
#         FROM public.rns_forbidden_word;
#     """
#     with get_connection() as conn:
#         with conn.cursor() as cur:
#             cur.execute(query)
#             rows = cur.fetchall()
#             return [row[0] for row in rows]

# # def fetch_forbidden_words(context_id: int = 2) -> list[str]:
# #     query = """
# #         SELECT keyword 
# #         FROM public.rns_forbidden_word f
# #         JOIN public.rns_forbidden_word_context fwc ON f.forbidden_word_id = fwc.forbidden_word_id
# #         WHERE fwc.context_id = %s;
# #     """
# #     with get_connection() as conn:
# #         with conn.cursor() as cur:
# #             cur.execute(query, (context_id,))
# #             rows = cur.fetchall()
# #             return [row[0] for row in rows]


# def fetch_products() -> pd.DataFrame:
#     query = """
#         SELECT product_id, product_name 
#         FROM public.rns_product
#         WHERE status_record != 'D'
#         LIMIT 1000;
#     """
#     with get_connection() as conn:
#         return pd.read_sql_query(query, conn)


# def save_to_parquet(df: pd.DataFrame, path: str):
#     df.to_parquet(path, index=False)