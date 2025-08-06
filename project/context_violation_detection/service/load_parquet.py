# project/context_violation_detection/service/load_parquet.py

import pandas as pd
from pathlib import Path

def load_products_and_keywords(parquet_dir: Path = Path("project/context_violation_detection/data/parquet")):
    products_df = pd.read_parquet(parquet_dir / "products.parquet")
    forbidden_df = pd.read_parquet(parquet_dir / "forbidden_words.parquet")

    products = list(products_df.itertuples(index=False, name=None))  # [(product_id, product_name), ...]
    forbidden_words = forbidden_df["keyword"].dropna().unique().tolist()

    return forbidden_words, products