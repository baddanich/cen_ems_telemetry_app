"""
ASGI app entry point. Used by production (uvicorn backend.app.main:app) and by tests
(import create_app for an ASGI app instance). Kept here so there is a single place
that wires lifespan, router, and config; tests import create_app from this module.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import api
from .db import connect, disconnect
from .logging_config import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    await connect()
    yield
    await disconnect()


def create_app() -> FastAPI:
    configure_logging()
    app = FastAPI(lifespan=lifespan, title="CenEMS Telemetry Service")
    app.include_router(api.router)
    return app


app = create_app()
