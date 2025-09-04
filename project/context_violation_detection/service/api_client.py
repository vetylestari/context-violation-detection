import asyncio
import re
import aiohttp
from aiolimiter import AsyncLimiter
from openai import BaseModel

from project.context_violation_detection.db.fetch import fetch_products, fetch_forbidden_words
from project.context_violation_detection.db.insert import insert_violations

limiter = AsyncLimiter(5, 1)  # max 5 requests per second

# async def call_api(session, url, payload):
#     async with limiter:
#         async with session.post(url, json=payload) as resp:
#             return await resp.json()

async def check_violation(product_id: int, product_name: str, forbidden_words: list[str]):
    matched = []
    for w in forbidden_words:
        pattern = r"\b" + re.escape(w.lower()) + r"\b"
        if re.search(pattern, product_name.lower()):
            matched.append(w)

    return {
        "product_id": product_id,
        "product_name": product_name,
        "matched_keywords": matched,
        "is_violation": bool(matched),
    }


async def run_all_checks(products, forbidden_words):
    tasks = [check_violation(product_id, product_name, forbidden_words) 
             for product_id, product_name in products]
    results = await asyncio.gather(*tasks)

    # simpan ke DB
    formatted_results = [
        (r["product_id"], r["product_name"], ",".join(r["matched_keywords"]), r["is_violation"])
        for r in results
    ]
    if formatted_results:
        insert_violations(formatted_results)

    return results