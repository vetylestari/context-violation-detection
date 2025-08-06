# project/context_violation_detection/service/save_to_parquet.py

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)

def save_violations_to_parquet(violations: list[dict], filename: str = "violating_products.parquet"):
    """
    Save violation detection results to a Parquet file.
    
    Each item in `violations` should be a dict with:
        - product_id: int
        - product_name: str
        - keyword: str
        - is_violation: bool
    """
    if not violations:
        logging.warning("No data to save")
        return

    df = pd.DataFrame(violations)
    
    # Output folder
    output_path = Path("project/context_violation_detection/sample_data")
    output_path.mkdir(parents=True, exist_ok=True)
    full_path = output_path / filename

    df.to_parquet(full_path, index=False)
    logging.info(f"Saved {len(df)} records to {full_path}")
