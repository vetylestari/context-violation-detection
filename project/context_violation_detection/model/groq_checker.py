# import openai
# from project.context_violation_detection.config.settings import GROQ_API_KEY

# openai.api_key = GROQ_API_KEY
# openai.api_base = "https://api.groq.com/openai/v1"  # Groq endpoint

# def check_violation(product_name: str, forbidden_words: list[str]) -> bool:
#     prompt = f"""
#     You are a content validator. Your job is to detect if a product name violates content policy by containing forbidden words in a harmful or inappropriate context.
    
#     Forbidden words: {", ".join(forbidden_words)}
    
#     Example (does NOT violate): 'kandang anjing'
#     Example (DOES violate): 'jual bokep murah'

#     Product name: '{product_name}'
    
#     Does it violate the content policy? Answer with "YES" or "NO" only.
#     """

#     response = openai.ChatCompletion.create(
#         model="mixtral-8x7b-32768",
#         messages=[{"role": "user", "content": prompt}],
#         temperature=0.1,
#         max_tokens=3,
#     )

#     return "YES" in response.choices[0].message["content"].upper()

import openai
import asyncio
from project.context_violation_detection.config.settings import GROQ_API_KEY

openai.api_key = GROQ_API_KEY
openai.api_base = "https://api.groq.com/openai/v1"

# Max concurrent Groq requests
semaphore = asyncio.Semaphore(5)  # adjust this based on testing

async def check_violation(product_id: str,product_name: str, keyword: str) -> dict:
    prompt = f"""
    You are a content validator. Your job is to detect if a product name violates content policy by containing forbidden words in a harmful or inappropriate context.

    Forbidden word: {keyword}

    Example (does NOT violate): 'kandang anjing'
    Example (DOES violate): 'jual bokep murah'

    Product name: '{product_name}'

    Does it violate the content policy? Answer with "YES" or "NO" only.
    """

    async with semaphore:
        try:
            response = await openai.ChatCompletion.acreate(
                model="mixtral-8x7b-32768",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=3,
            )
            content = response.choices[0].message["content"].strip().upper()
            return {
                "product_id": product_id,
                "product_name": product_name,
                "keyword": keyword,
                "violation": content == "YES"
            }
        except Exception as e:
            return {
                "product_id": product_id,
                "product_name": product_name,
                "keyword": keyword,
                "violation": None,
                "error": str(e)
            }
            
async def run_all_checks(products: list[tuple[int, str]], keywords: list[str]) -> list[dict]:
    tasks = []
    for product_id, product_name in products:
        for keyword in keywords:
            tasks.append(check_violation(product_id, product_name, keyword))
    results = await asyncio.gather(*tasks)
    return results