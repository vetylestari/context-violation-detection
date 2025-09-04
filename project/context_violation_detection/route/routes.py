# # project/context_violation_detection/route/routes.py

# import sys
# import logging
# import asyncpg
# import os
# from pathlib import Path
# from fastapi import APIRouter, Depends, Request, status, HTTPException
# from fastapi.responses import JSONResponse
# from dotenv import dotenv_values
# from decouple import config as conf
# from context_violation_detection.service.api import get_violations
# from context_violation_detection.service.api_client import run_all_checks, CheckRangeRequest
# from pydantic import BaseModel
# from typing import List

# from project.context_violation_detection.db.fetch import fetch_products_range, fetch_forbidden_words

# class CheckRangeRequest(BaseModel):
#     start_id: int
#     end_id: int

# # === ENV Setup ===
# BASE_PATH = Path(__file__).resolve().parent.parent.parent
# ENV_PATH = BASE_PATH / ".env"

# if ENV_PATH.exists():
#     ENV = dotenv_values(ENV_PATH)
#     DB_DSN = ENV.get("DATABASE_URL", "")
# else:
#     DB_DSN = conf("DATABASE_URL", default="")

# if not DB_DSN:
#     raise ValueError("DATABASE_URL is not set")

# # === Logger Setup ===
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[logging.StreamHandler(sys.stdout)],
# )
# logger = logging.getLogger(__name__)

# # === FastAPI Router ===
# router = APIRouter()

# # === DB Connection Dependency ===
# async def get_db():
#     conn = await asyncpg.connect(DB_DSN)
#     try:
#         yield conn
#     finally:
#         await conn.close()
#         logger.info("DB connection closed")

# # === Request Model ===
# class Product(BaseModel):
#     product_id: int
#     product_name: str

# class ViolationRequest(BaseModel):
#     products: List[Product]
#     keywords: List[str]

# # === Endpoint ===
# @router.post("/check_violations/")
# async def check_violations():
#     try:
#         violations = await get_violations()
#         return JSONResponse(content={"violations": violations}, status_code=status.HTTP_200_OK)
#     except Exception as e:
#         logger.exception("Error during violation check")
#         raise HTTPException(status_code=500, detail=str(e))
    
# @router.post("/run_violation_detection/")
# async def run_violation_detection_endpoint(request: ViolationRequest):
#     try:
#         # passing ke service
#         result = await run_all_checks(
#             products=request.products,
#             keywords=request.keywords,
#             url="http://localhost:8080/check_violations/"
#         )
#         return JSONResponse(content={"results": result}, status_code=200)
#     except Exception as e:
#         logger.exception("Failed to run violation detection")
#         raise HTTPException(status_code=500, detail=str(e))

# @router.post("/check_range")
# async def check_range(request: CheckRangeRequest):
#     # 1. fetch products
#     df_products = fetch_products_range(request.start_id, request.end_id)
#     products = list(df_products.itertuples(index=False, name=None))  # [(id, name), ...]

#     # 2. fetch forbidden words
#     forbidden_words = fetch_forbidden_words()

#     # 3. run checks
#     results = await run_all_checks(products, forbidden_words)

#     return {"results": results}

from fastapi import APIRouter, Depends, status, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import logging, sys
from pathlib import Path
from dotenv import dotenv_values
from decouple import config as conf

from project.context_violation_detection.db.fetch import fetch_products_range, fetch_forbidden_words
from project.context_violation_detection.service.api import get_violations
from project.context_violation_detection.service.api_client import run_all_checks

# === ENV Setup ===
BASE_PATH = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_PATH / ".env"
ENV = dotenv_values(ENV_PATH) if ENV_PATH.exists() else {}
DB_DSN = ENV.get("DATABASE_URL", conf("DATABASE_URL", default=""))
if not DB_DSN:
    raise ValueError("DATABASE_URL is not set")

# === Logger Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# === FastAPI Router ===
router = APIRouter()

# === Request Models ===
class Product(BaseModel):
    product_id: int
    product_name: str

class ViolationRequest(BaseModel):
    products: List[Product]
    keywords: List[str]

class CheckRangeRequest(BaseModel):
    start_id: int
    end_id: int


# === Endpoint ===
@router.post("/check_violations/")
async def check_violations():
    try:
        violations = await get_violations()
        return {"violations": violations}
    except Exception as e:
        logger.exception("Error during violation check")
        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/run_violation_detection/")
async def run_violation_detection_endpoint(request: ViolationRequest):
    try:
        result = await run_all_checks(
            products=[(p.product_id, p.product_name) for p in request.products],
            forbidden_words=request.keywords
        )
        return {"results": result}
    except Exception as e:
        logger.exception("Failed to run violation detection")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/check_range")
async def check_range(request: CheckRangeRequest):
    # 1. fetch products
    df_products = fetch_products_range(request.start_id, request.end_id)
    products = list(df_products.itertuples(index=False, name=None))  # [(id, name), ...]

    # 2. fetch forbidden words
    forbidden_words = fetch_forbidden_words()

    # 3. run checks
    results = await run_all_checks(products, forbidden_words)

    return {"results": results}