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
import json
from project.context_violation_detection.config.settings import GROQ_API_KEY

openai.api_key = GROQ_API_KEY
openai.api_base = "https://api.groq.com/openai/v1"

# Max concurrent Groq requests
semaphore = asyncio.Semaphore(5)  # adjust this based on testing

def build_prompt(product_name: str, keyword: str) -> str:
    return f"""
            You are a content validator for an online marketplace.

            Your job is to determine whether a product name contains the given forbidden keyword in a **harmful, inappropriate, or policy-violating context**.

            You will be given:
            - A product name
            - A forbidden keyword

            Respond in the following JSON format:
            {{
            "is_violation": true or false,
            "reason": "short explanation why it is or is not a violation"
            }}

            Examples:
            - Product name: "kandang anjing", keyword: "anjing" → {{ "is_violation": false, "reason": "Used in a neutral context" }}
            - Product name: "jual video bokep murah", keyword: "bokep" → {{ "is_violation": true, "reason": "Contains explicit content" }}

            Now evaluate:
            - Product name: "{product_name}"
            - Forbidden keyword: "{keyword}"
        """.strip()

import json

async def check_violation(product_id: str, product_name: str, keyword: str) -> dict:
    prompt = f"""
You are a content policy checker. Your job is to evaluate whether the given product name contains the keyword in a potentially inappropriate or policy-violating context.

Product name: "{product_name}"
Keyword: "{keyword}"

Respond in JSON format:
{{
  "is_violation": true or false,
  "reason": "short reason why or why not"
}}
""".strip()

    async with semaphore:
        try:
            response = await openai.ChatCompletion.acreate(
                model="mixtral-8x7b-32768",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=100,
            )
            content = response.choices[0].message["content"].strip()
            result = json.loads(content)

            return {
                "product_id": product_id,
                "product_name": product_name,
                "keyword": keyword,
                "violation": result.get("is_violation", False),
                "reason": result.get("reason", "")
            }
        except Exception as e:
            return {
                "product_id": product_id,
                "product_name": product_name,
                "keyword": keyword,
                "violation": None,
                "reason": f"LLM ERROR: {str(e)}"
            }

async def run_all_checks(products: list[tuple[int, str]], keywords: list[str]) -> list[dict]:
    tasks = []
    for product_id, product_name in products:
        for keyword in keywords:
            tasks.append(check_violation(product_id, product_name, keyword))
    results = await asyncio.gather(*tasks)
    return results