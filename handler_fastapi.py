import os
import uvicorn
import importlib
from fastapi import APIRouter, FastAPI, status
from pydantic import BaseModel

dir_path = f"{os.path.dirname(os.path.realpath(__file__))}/project"
project_list = os.listdir(dir_path)

class HealthCheck(BaseModel):
    """Response model to validate and return when performing a health check."""
    status: str = "OK"


app = FastAPI(
    title="Renos DataAPI",
    description="",
    summary="",
    version="2.0.0",
    terms_of_service="",
    contact={
        "name": "Vety Bhakti Lestari",
        "email": "vety.lestari@renos.id",
    },
)

router = APIRouter(
    prefix="/dataapi/violation-detection",
    tags=["violation-detection"],
    responses={404: {"description": "Not found"}},)

@app.get(
    "/health",
    tags=["System"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
def healthcheck():
    return HealthCheck(status="OK")

def run():
    uvicorn.run("handler_fastapi:app", host="0.0.0.0", port=8080, reload=True)

def project_import(project_name):
    path = "project"
    module = project_name
    globals()[module] = importlib.import_module(f"{path}.{module}.main")
    app.include_router(globals()[module].router)

for pl in project_list:
    full_path = os.path.join(dir_path, pl)
    if (
        os.path.isdir(full_path) and
        not pl.startswith("__") and
        not pl.endswith(".md")
    ):
        try:
            project_import(pl)
            print(f"SUCCESS ON IMPORTING '{pl}'")
        except Exception as e:
            print(f"FAILED TO IMPORT '{pl}'")
            print(f"Error: {str(e)}")
    # project_import(pl)
    
app.include_router(router)
    
    