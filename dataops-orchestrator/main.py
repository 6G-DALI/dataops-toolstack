from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import rabbitmq_consumer
from config import HOST, PORT, CORS_ORIGINS
from routers import dags, runs, tasks, datasets, stats, services


@asynccontextmanager
async def lifespan(app: FastAPI):
    rabbitmq_consumer.start()
    yield
    await rabbitmq_consumer.stop()


app = FastAPI(
    title="DataOps Orchestrator",
    description="Middleware API between the React UI and Apache Airflow",
    version="0.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    # Response headers, which `allow_headers` does not cover — that one is about
    # the *request*. Without this a browser sees only the CORS-safelisted six, so
    # every X-Artifact-* header reads back as null: the artifact walk in
    # dataops-ui could not tell a truncated window from a complete file and
    # stopped after the first one, showing a prefix of a frame as if it were all
    # of it. Anything this API means a browser to read has to be listed here.
    expose_headers=[
        "X-Artifact-Key",
        "X-Artifact-Total-Size",
        "X-Artifact-Truncated",
        "X-Artifact-Offset",
        "X-Artifact-Next-Offset",
    ],
)

app.include_router(dags.router)
app.include_router(runs.router)
app.include_router(tasks.router)
app.include_router(datasets.router)
app.include_router(stats.router)
app.include_router(services.router)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
