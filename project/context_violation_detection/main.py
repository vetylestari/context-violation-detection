# project/context_violation_detection/main.py

from fastapi import FastAPI
from project.context_violation_detection.route.routes import router as cv_router

app = FastAPI(
    title="Context-Violation Detection",
    version="0.1.0"
)

app.include_router(cv_router, prefix="/datapi/context_violation_detection")

router = cv_router