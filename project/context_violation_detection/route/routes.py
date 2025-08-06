# project/context_violation_detection/route/routes.py

import sys
import logging
import asyncpg
import os
from pathlib import Path
from fastapi import APIRouter, Depends, status, HTTPException
from fastapi.responses import JSONResponse
from dotenv import dotenv_values
from decouple import config as conf
from project.context_violation_detection.service.api import get_violations
from project.context_violation_detection.task.runner import run_violation_detection


# === ENV Setup ===
BASE_PATH = Path(__file__).resolve().parent.parent.parent
ENV_PATH = BASE_PATH / ".env"

if ENV_PATH.exists():
    ENV = dotenv_values(ENV_PATH)
    DB_DSN = ENV.get("DATABASE_URL", "")
else:
    DB_DSN = conf("DATABASE_URL", default="")

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

# === DB Connection Dependency ===
async def get_db():
    conn = await asyncpg.connect(DB_DSN)
    try:
        yield conn
    finally:
        await conn.close()
        logger.info("DB connection closed")

# === Endpoint ===
@router.post("/check_violations/")
async def check_violations():
    try:
        violations = await get_violations()
        return JSONResponse(content={"violations": violations}, status_code=status.HTTP_200_OK)
    except Exception as e:
        logger.exception("Error during violation check")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/run_violation_detection/")
async def run_violation_detection_endpoint():
    try:
        result = await run_violation_detection()
        return JSONResponse(content=result, status_code=200)
    except Exception as e:
        logger.exception("Failed to run violation detection")
        raise HTTPException(status_code=500, detail=str(e))
