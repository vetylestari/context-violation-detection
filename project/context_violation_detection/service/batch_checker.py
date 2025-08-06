# project/context_violation_detection/service/batch_checker.py

import time
import logging
import pandas as pd
from multiprocessing import Pool, cpu_count
from functools import partial
from pathlib import Path

from project.context_violation_detection.model.groq_checker import check_violation
from project.context_violation_detection.service.save_to_parquet import save_violations_to_parquet

logging.basicConfig(level=logging.INFO)

# === Constants ===
DATA_DIR = Path("data/parquet")
PRODUCTS_FILE = DATA_DIR / "products.parquet"
FORBIDDEN_WORDS_FILE = DATA_DIR / "forbidden_words.parquet"

# === Worker Function ===
def process_entry(forbidden_words: list[str], entry: tuple[int, str]) -> list[dict]:
    product_id, product_name = entry
    results = []

    for keyword in forbidden_words:
        if keyword.lower() in product_name.lower():
            is_violation = check_violation(product_name, [keyword])
            logging.info(f"[{product_id}] '{product_name[:50]}...' | Keyword: '{keyword}' -> {is_violation}")
            results.append({
                "product_id": product_id,
                "product_name": product_name,
                "keyword": keyword,
                "is_violation": is_violation
            })
            time.sleep(1.2)  # Rate limit buffer

    return results

# === Main Function ===
def run_parallel_checker():
    logging.info("📦 Loading parquet files...")
    if not PRODUCTS_FILE.exists() or not FORBIDDEN_WORDS_FILE.exists():
        logging.error("❌ Required parquet files not found. Run prepare_data.py first.")
        return

    products_df = pd.read_parquet(PRODUCTS_FILE)
    forbidden_df = pd.read_parquet(FORBIDDEN_WORDS_FILE)

    products = list(products_df.itertuples(index=False, name=None))  # (product_id, product_name)
    forbidden_words = forbidden_df['keyword'].tolist()

    logging.info(f"✅ Loaded {len(products)} products and {len(forbidden_words)} forbidden words")

    with Pool(processes=cpu_count() - 1 or 1) as pool:
        logging.info(f"🚀 Running with {cpu_count() - 1 or 1} worker(s)...")

        # Use partial to pass forbidden_words to worker
        worker = partial(process_entry, forbidden_words)

        all_results = pool.map(worker, products)

    flattened = [violation for sublist in all_results for violation in sublist]
    logging.info(f"✅ Detected {len(flattened)} total potential violations")

    save_violations_to_parquet(flattened)

# === CLI entry ===
if __name__ == "__main__":
    run_parallel_checker()