# import pandas as pd
# from pathlib import Path
# from project.context_violation_detection.db.fetch import (
#     fetch_products,
#     fetch_forbidden_words,
#     save_to_parquet,
# )

# def load_or_fetch_products(csv_path, parquet_path):
#     if csv_path.exists():
#         print("📄 Loading products from CSV...")
#         df = pd.read_csv(csv_path)
#         save_to_parquet(df, parquet_path)
#         print(f"✅ Saved {len(df)} products to {parquet_path}")
#     else:
#         print("📦 Fetching products from database...")
#         df = fetch_products()
#         save_to_parquet(df, parquet_path)
#         print(f"✅ Saved {len(df)} products to {parquet_path}")
#     return df

# def load_or_fetch_forbidden_words(csv_path, parquet_path):
#     if csv_path.exists():
#         print("📄 Loading forbidden words from CSV...")
#         df = pd.read_csv(csv_path)
#         save_to_parquet(df, parquet_path)
#         print(f"✅ Saved {len(df)} forbidden words to {parquet_path}")
#     else:
#         print("📦 Fetching forbidden words from database...")
#         fw = fetch_forbidden_words()
#         df = pd.DataFrame(fw, columns=["keyword"])
#         save_to_parquet(df, parquet_path)
#         print(f"✅ Saved {len(df)} forbidden words to {parquet_path}")
#     return df

# def main():
#     output_dir = Path("project/context_violation_detection/data/parquet")
#     output_dir.mkdir(parents=True, exist_ok=True)

#     # Paths to CSVs
#     products_csv = Path("project/context_violation_detection/sample_data/Product List 200.csv")
#     products_parquet = output_dir / "products.parquet"

#     fw_csv = Path("project/context_violation_detection/sample_data/Keyword Forbidden Word.csv")
#     fw_parquet = output_dir / "forbidden_words.parquet"

#     # Load or fetch and save to parquet
#     products_df = load_or_fetch_products(products_csv, products_parquet)
#     fw_df = load_or_fetch_forbidden_words(fw_csv, fw_parquet)

# if __name__ == "__main__":
#     main()
    
#----------------------------------------------------------------------------------------


import pandas as pd
from pathlib import Path
from project.context_violation_detection.db.fetch import (
    fetch_products,
    fetch_forbidden_words,
    save_to_parquet,
)

def main():
    output_dir = Path("data/parquet")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("📦 Fetching products...")
    products_df = fetch_products()
    save_to_parquet(products_df, output_dir / "products.parquet")
    print(f"✅ Saved {len(products_df)} products to {output_dir/'products.parquet'}")

    print("📦 Fetching forbidden words...")
    forbidden_words = fetch_forbidden_words()
    fw_df = pd.DataFrame(forbidden_words, columns=["keyword"])
    save_to_parquet(fw_df, output_dir / "forbidden_words.parquet")
    print(f"✅ Saved {len(fw_df)} forbidden words to {output_dir/'forbidden_words.parquet'}")


if __name__ == "__main__":
    main()