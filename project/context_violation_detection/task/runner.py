import asyncio
import logging
from project.context_violation_detection.db.insert import insert_violations
from project.context_violation_detection.model.groq_checker import run_all_checks
from project.context_violation_detection.service.load_parquet import load_products_and_keywords

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Loading data from parquet...")
    forbidden_words, products = load_products_and_keywords()

    logger.info(f"Total products: {len(products)}, Forbidden words: {len(forbidden_words)}")

    logger.info("Running violation checks...")
    results = asyncio.run(run_all_checks(products, forbidden_words))

    violations = [r for r in results if r.get("violation") is True]
    logger.info(f"Detected {len(violations)} violating products. Inserting into database...")
    insert_violations(violations)
    logger.info("Done. All violations inserted.")


async def run_violation_detection():
    logger.info("Loading data from parquet...")
    forbidden_words, products = load_products_and_keywords()

    logger.info(f"Total products: {len(products)}, Forbidden words: {len(forbidden_words)}")

    logger.info("Running violation checks...")
    results = await run_all_checks(products, forbidden_words)

    violations = [r for r in results if r.get("violation") is True]
    logger.info(f"Detected {len(violations)} violating products. Inserting into database...")
    insert_violations(violations)
    logger.info("Done. All violations inserted.")

    return {"total_checked": len(products), "violations": len(violations)}


if __name__ == "__main__":
    main()