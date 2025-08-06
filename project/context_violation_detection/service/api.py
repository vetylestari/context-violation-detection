from typing import List, Dict
from pathlib import Path
import pandas as pd

from project.context_violation_detection.model.groq_checker import run_all_checks
from project.context_violation_detection.service.prepare_data import main as prepare_parquet

PARQUET_DIR = Path("data/parquet")

def load_parquet_data() -> tuple[list[str], list[tuple[int, str]]]:
    """
    Load forbidden words and product list from local parquet files.
    If missing, regenerate them from database using prepare_data.py logic.
    """
    products_path = PARQUET_DIR / "products.parquet"
    forbidden_path = PARQUET_DIR / "forbidden_words.parquet"

    # Auto-generate if missing
    if not products_path.exists() or not forbidden_path.exists():
        print("⚠️ Parquet files not found. Generating...")
        prepare_parquet()

    products_df = pd.read_parquet(products_path)
    forbidden_df = pd.read_parquet(forbidden_path)

    products = list(products_df[['product_id', 'product_name']].itertuples(index=False, name=None))
    forbidden_words = forbidden_df['keyword'].tolist()

    return forbidden_words, products

async def get_violations() -> List[Dict[str, str]]:
    """
    Load data from parquet, run checks, and return violations only.
    """
    forbidden_words, products = load_parquet_data()

    results = await run_all_checks(products, forbidden_words)

    violations = [
        {
            "product_id": r["product_id"],
            "product_name": r["product_name"],
            "keyword": r["keyword"]
        }
        for r in results if r.get("violation") is True
    ]

    return violations