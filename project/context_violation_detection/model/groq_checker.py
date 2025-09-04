# # import openai
# # from project.context_violation_detection.config.settings import GROQ_API_KEY

# # openai.api_key = GROQ_API_KEY
# # openai.api_base = "https://api.groq.com/openai/v1"  # Groq endpoint

# # def check_violation(product_name: str, forbidden_words: list[str]) -> bool:
# #     prompt = f"""
# #     You are a content validator. Your job is to detect if a product name violates content policy by containing forbidden words in a harmful or inappropriate context.
    
# #     Forbidden words: {", ".join(forbidden_words)}
    
# #     Example (does NOT violate): 'kandang anjing'
# #     Example (DOES violate): 'jual bokep murah'

# #     Product name: '{product_name}'
    
# #     Does it violate the content policy? Answer with "YES" or "NO" only.
# #     """

# #     response = openai.ChatCompletion.create(
# #         model="mixtral-8x7b-32768",
# #         messages=[{"role": "user", "content": prompt}],
# #         temperature=0.1,
# #         max_tokens=3,
# #     )

# #     return "YES" in response.choices[0].message["content"].upper()

# import asyncio
# from itertools import product
# import json
# from project.context_violation_detection.config.settings import GROQ_API_KEY
# from context_violation_detection.db.insert import insert_violations
# from openai import OpenAI
# from decouple import config

# client = OpenAI(
#     api_key=config("GROQ_API_KEY"),
#     base_url="https://api.groq.com/openai/v1"
# )

# # Max concurrent Groq requests
# semaphore = asyncio.Semaphore(5)  # adjust this based on testing
# def build_prompt(product_id: int, product_name: str, keywords: list[str]) -> dict:
#     return f"""
#         Kamu adalah sistem moderasi produk berbasis konteks.

#         Tugas:
#         - Periksa apakah nama produk berikut mengandung salah satu kata dari daftar forbidden.
#         - Jika ada kata yang cocok, tentukan apakah konteks penggunaannya melanggar atau tidak.
#         - Jika ada kata yang cocok tapi konteksnya normal/aman, tetap tampilkan di matched_keywords tapi is_violation = false.

#         Aturan:
#         1. Jika kata digunakan dalam konteks normal (misalnya nama produk, deskripsi fungsional), maka "is_violation" = false.
#         2. Jika kata digunakan sebagai umpatan, penghinaan, atau dalam konteks negatif, maka "is_violation" = true.

#         Format output JSON (wajib sesuai):
#         {{
#         "product_id": {product_id},
#         "product_name": "{product_name}",
#         "matched_keywords": ["keyword1", "keyword2"],
#         "is_violation": true
#         }}

#         Sekarang evaluasi produk berikut:

#         product_name: "{product_name}"
#         forbidden_keywords: {keywords}
# """.strip()


# async def check_violation(product_id: int, product_name: str, keywords: list[str]) -> dict:
#     prompt = build_prompt(product_id, product_name, keywords)
#     try:
#         loop = asyncio.get_event_loop()
#         response = await loop.run_in_executor(
#             None,
#             lambda: client.chat.completions.create(
#                 model="llama-3.1-8b-instant",
#                 messages=[
#                     {"role": "system", "content": "Kamu adalah sistem validasi forbidden word."},
#                     {"role": "user", "content": prompt}
#                 ],
#                 temperature=0
#             )
#         )
#         content = response.choices[0].message.content.strip()
#         print(f"[DEBUG] Raw LLM response for {product_name}: {content}")

#         result = json.loads(content)
#         return {
#             "product_id": product_id,
#             "product_name": product_name,
#             "matched_keywords": result.get("matched_keywords", []),
#             "is_violation": result.get("is_violation", False),
#         }

#     except Exception as e:
#         return {
#             "product_id": product_id,
#             "product_name": product_name,
#             "matched_keywords": [],
#             "is_violation": False,
#             "error": str(e)
#         }



# def format_results_for_db(results: list[dict]) -> list[tuple]:
#     formatted = []
#     for r in results:
#         formatted.append((
#             r["product_id"],
#             r["product_name"],
#             r["keyword"],
#             r["is_violation"]
#         ))
#     return formatted


# async def run_all_checks(products: list[tuple[int, str]], keywords: list[str]) -> list[dict]:
#     tasks = []
#     for product_id, product_name in products:
#         tasks.append(check_violation(product_id, product_name, keywords))  # <– sekali per produk
    
#     results = await asyncio.gather(*tasks)

#     # format untuk DB
#     formatted_results = [
#         (r["product_id"], r["product_name"], ",".join(r["matched_keywords"]), r["is_violation"])
#         for r in results
#     ]

#     if formatted_results:
#         insert_violations(formatted_results)

#     return results

import asyncio
import json
from project.context_violation_detection.config.settings import GROQ_API_KEY
from context_violation_detection.db.insert import insert_violations
from openai import OpenAI
from decouple import config

client = OpenAI(
    api_key=config("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1"
)

# === Prompt Builder ===
def build_prompt(product_name: str, keywords: list[str]) -> str:
    return f"""
        Kamu adalah sistem moderasi produk berbasis konteks.

        Tugas:
        - Periksa apakah nama produk berikut mengandung salah satu kata dari daftar forbidden.
        - Jika ada kata yang cocok, tentukan apakah konteks penggunaannya melanggar atau tidak.
        - Jika ada kata yang cocok tapi konteksnya normal/aman, tetap tampilkan di matched_keywords tapi is_violation = false.

        Aturan:
        1. Jika kata digunakan dalam konteks normal (misalnya bagian dari nama produk, deskripsi fungsional, atau kode model), maka "is_violation" = false.
        2. Jika kata digunakan sebagai umpatan, penghinaan, atau indikasi barang palsu/ilegal/negatif, maka "is_violation" = true.
        3. Abaikan kecocokan palsu yang hanya substring dari kata lain (contoh: "premium" tidak dianggap "remi", "KWD155HC" tidak dianggap "kw").
        4. Hanya beri is_violation = true bila benar-benar ada konteks pelanggaran.
        5. Utamakan analisis makna keseluruhan kalimat, jangan hanya mencocokkan kata secara literal (contoh: "pembunuh virus" tidak dianggap "pembunuh" karena konteksnya tidak mengandung kekerasan atau tindakan membahayakan).

        Format output JSON (wajib sesuai):
        {
            "product_name": "{product_name}",
            "matched_keywords": ["keyword1", "keyword2"],
            "is_violation": true/false
        }

        Sekarang evaluasi produk berikut:

        product_name: "{product_name}"
        forbidden_keywords: {keywords}
    """.strip()

# === Check Violation ===
# async def check_violation(product_id: int, product_name: str, keywords: list[str]) -> dict:
#     prompt = build_prompt(product_name, keywords)

#     try:
#         loop = asyncio.get_event_loop()
#         response = await loop.run_in_executor(
#             None,
#             lambda: client.chat.completions.create(
#                 model="llama-3.1-8b-instant",
#                 messages=[
#                     {"role": "system", "content": "Kamu adalah sistem validasi forbidden word."},
#                     {"role": "user", "content": prompt}
#                 ],
#                 temperature=0
#             )
#         )

#         content = response.choices[0].message.content.strip()
#         print(f"[DEBUG] Raw LLM response for {product_name}: {content}")

#         try:
#             result = json.loads(content)
#         except json.JSONDecodeError:
#             # fallback kalau gak valid JSON
#             result = {"matched_keywords": [], "is_violation": False}

#         return {
#             "product_id": product_id,
#             "product_name": product_name,
#             "matched_keywords": result.get("matched_keywords", []),
#             "is_violation": result.get("is_violation", False)
#         }

#     except Exception as e:
#         return {
#             "product_id": product_id,
#             "product_name": product_name,
#             "matched_keywords": [],
#             "is_violation": False,
#             "error": str(e)
#         }

async def check_violation(product_id: int, product_name: str, keywords: list[str]) -> dict:
    prompt = build_prompt(product_name, keywords)

    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": "Kamu adalah sistem validasi forbidden word."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0
            )
        )

        content = response.choices[0].message.content.strip()
        print(f"[DEBUG] Raw LLM response for {product_name}: {content}")

        try:
            result = json.loads(content)
        except json.JSONDecodeError:
            # fallback kalau gak valid JSON
            result = {"matched_keywords": [], "is_violation": False}

        # --- Post-process safety rules ---
        matched = result.get("matched_keywords", [])
        violation = result.get("is_violation", False)

        safe_patterns = [
            ("remi", "premium"),      # "remi" dalam "premium"
            ("kw", "kwd"),            # "kw" dalam "KWD155HC"
            ("pembunuh", "virus"),    # "pembunuh" dalam "pembunuh virus"
        ]

        for kw in matched:
            pname_lower = product_name.lower()
            for bad, safe in safe_patterns:
                if kw == bad and safe in pname_lower:
                    print(f"[SAFE_RULE] Override: {kw} dianggap aman di {product_name}")
                    violation = False

        return {
            "product_id": product_id,
            "product_name": product_name,
            "matched_keywords": matched,
            "is_violation": violation
        }

    except Exception as e:
        return {
            "product_id": product_id,
            "product_name": product_name,
            "matched_keywords": [],
            "is_violation": False,
            "error": str(e)
        }


# === Run All Checks ===
async def run_all_checks(products: list[tuple[int, str]], keywords: list[str]) -> list[dict]:
    tasks = [check_violation(product_id, product_name, keywords) for product_id, product_name in products]
    results = await asyncio.gather(*tasks)

    # format untuk DB
    formatted_results = [
        (r["product_id"], r["product_name"], ",".join(r["matched_keywords"]), r["is_violation"])
        for r in results
    ]

    if formatted_results:
        insert_violations(formatted_results)

    return results
