import logging
import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.middleware.cors import CORSMiddleware

import rabbitmq_consumer
from config import HOST, PORT, CORS_ORIGINS, EDC_PROVIDER_MANAGEMENT_URL
from routers import dags, runs, tasks, datasets, stats, services, testbeds


# Uvicorn only configures its own loggers; without this our application logs (the "[edc] ..." lines)
# would not appear in `docker logs` below WARNING.
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(levelname)s:     %(name)s: %(message)s")
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Central connector management API: %s", EDC_PROVIDER_MANAGEMENT_URL or "(not configured)")
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
app.include_router(testbeds.router)


@app.exception_handler(StarletteHTTPException)
async def upstream_failures(request: Request, exc: StarletteHTTPException):
    """The testbed registry talks to other systems (central connector, MinIO, piveau). When one of them
    fails, the registry answers 424 Failed Dependency instead of 502/503/504: the production edge in
    front of the UI replaces every 5xx body with a generic "Service temporarily unavailable" page,
    which hid the real error. The original status travels in `upstream_status`."""
    if request.url.path.startswith("/testbeds") and exc.status_code in (502, 503, 504):
        log.warning("%s %s -> %s: %s", request.method, request.url.path, exc.status_code, exc.detail)
        return JSONResponse({"detail": exc.detail, "upstream_status": exc.status_code}, status_code=424)
    return await http_exception_handler(request, exc)


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT, reload=True)
