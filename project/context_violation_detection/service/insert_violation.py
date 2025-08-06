# project/context_violation_detection/service/insert_violation.py

import pandas as pd
import logging
from pathlib import Path
from project.context_violation_detection.db.insert import insert_violations

logging.basicConfig(level=logging.INFO)

def main():
    parquet_path = Path("project/context_violation_detection/sample_data/violating_products.parquet")

    if not parquet_path.exists():
        logging.error(f"Parquet file not found: {parquet_path}")
        return

    df = pd.read_parquet(parquet_path)
    logging.info(f"Loaded {len(df)} rows from parquet")

    # Prepare records
    records = []
    for _, row in df.iterrows():
        if pd.isna(row["keyword"]):  # optional check
            continue
        records.append((
            int(row["product_id"]),
            str(row["product_name"]),
            str(row["keyword"]),
            bool(row["is_violation"])
        ))

    if records:
        insert_violations(records)
        logging.info(f"Inserted {len(records)} records into DB")
    else:
        logging.warning("No valid records to insert")

if __name__ == "__main__":
    main()