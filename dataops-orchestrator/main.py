import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import HOST, PORT, CORS_ORIGINS
from routers import dags, runs, tasks, datasets, stats

app = FastAPI(
    title="DataOps Orchestrator",
    description="Middleware API between the React UI and Apache Airflow",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dags.router)
app.include_router(runs.router)
app.include_router(tasks.router)
app.include_router(datasets.router)
app.include_router(stats.router)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
