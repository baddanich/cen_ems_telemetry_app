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
