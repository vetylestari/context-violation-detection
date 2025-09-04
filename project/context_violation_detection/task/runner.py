# import asyncio
# import logging
# from project.context_violation_detection.db.insert import insert_violations
# from project.context_violation_detection.model.groq_checker import run_all_checks
# from project.context_violation_detection.service.load_parquet import load_products_and_keywords
# import asyncio
# import aiohttp
# from aiolimiter import AsyncLimiter

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger(__name__)


# def main():
#     logger.info("Loading data from parquet...")
#     forbidden_words, products = load_products_and_keywords()
#     logger.info(f"Total products: {len(products)}, Forbidden words: {len(forbidden_words)}")

#     logger.info("Running violation checks...")
#     results = asyncio.run(run_all_checks(products, forbidden_words))

#     total_violations = sum(1 for r in results if r.get("violation"))
#     logger.info(
#         f"Check completed. Total products: {len(results)}, "
#         f"Violating: {total_violations}, Clean: {len(results) - total_violations}"
#     )

#     logger.info("Inserting products into database (with violation flags)...")
#     insert_violations(results)
#     logger.info("Done. All products inserted.")


# limiter = AsyncLimiter(5, 1)  # max 5 requests per second

# async def call_api(session, url, payload):
#     async with limiter:  
#         async with session.post(url, json=payload) as resp:
#             return await resp.json()

# async def check_violation(session, product, forbidden_words, url):
#     payload = {
#         "product": product,
#         "forbidden_words": forbidden_words
#     }
#     try:
#         response = await call_api(session, url, payload)
#         return response
#     except Exception as e:
#         return {"error": str(e), "product": product}

# async def run_all_checks(products, forbidden_words):
#     url = "http://localhost:8000/check"  # contoh API endpoint
#     async with aiohttp.ClientSession() as session:
#         tasks = [
#             check_violation(session, p, forbidden_words, url) 
#             for p in products
#         ]
#         return await asyncio.gather(*tasks)

import asyncio
import logging
from context_violation_detection.db.insert import insert_violations
from context_violation_detection.model.groq_checker import run_all_checks
from context_violation_detection.service.load_parquet import load_products_and_keywords

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def main():
    logger.info("Loading data from parquet...")
    forbidden_words, products = load_products_and_keywords()
    logger.info(f"Total products: {len(products)}, Forbidden words: {len(forbidden_words)}")

    logger.info("Running violation checks...")
    results = asyncio.run(run_all_checks(products, forbidden_words))

    total_violations = sum(1 for r in results if r.get("violation"))
    logger.info(
        f"Check completed. Total products: {len(results)}, "
        f"Violating: {total_violations}, Clean: {len(results) - total_violations}"
    )

    logger.info("Inserting products into database (with violation flags)...")
    insert_violations(results)
    logger.info("Done. All products inserted.")

if __name__ == "__main__":
    main()
