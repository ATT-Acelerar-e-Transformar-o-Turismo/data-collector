import asyncio
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from dependencies.rabbitmq import data_mq_client, services_mq_client
from routes import router as api_router
import logging
from contextlib import asynccontextmanager

import services.data_ingestor

logger = logging.getLogger(__name__)

# Watchdog: probe the MQ connections every WATCHDOG_INTERVAL seconds; if they
# fail FAILURE_THRESHOLD times in a row, exit the process so Docker (restart:
# always) re-creates the container with fresh connections. This is the
# backstop for aio_pika's robust-connection failing silently — we've seen
# the consumer loop sit on a dead iterator for minutes otherwise.
WATCHDOG_INTERVAL = 30
WATCHDOG_FAILURE_THRESHOLD = 3


async def _mq_watchdog():
    failures = 0
    while True:
        try:
            await asyncio.sleep(WATCHDOG_INTERVAL)
            data_ok = await data_mq_client.health_probe()
            services_ok = await services_mq_client.health_probe()
            if data_ok and services_ok:
                if failures:
                    logger.info(f"Watchdog: connections recovered after {failures} failure(s)")
                failures = 0
                continue
            failures += 1
            logger.warning(
                f"Watchdog: probe failed ({failures}/{WATCHDOG_FAILURE_THRESHOLD}) "
                f"data_mq={data_ok} services_mq={services_ok}"
            )
            if failures >= WATCHDOG_FAILURE_THRESHOLD:
                logger.error("Watchdog: MQ unrecoverable, exiting for Docker restart")
                os._exit(1)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.exception(f"Watchdog: unexpected error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    watchdog_task = None
    try:
        await data_mq_client.connect()
        await services_mq_client.connect()
        await data_mq_client.start_consumers()
        await services_mq_client.start_consumers()
        logger.info("RabbitMQ clients initialized successfully")
        watchdog_task = asyncio.create_task(_mq_watchdog())
        yield
    finally:
        if watchdog_task:
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
        await data_mq_client.close()
        await services_mq_client.close()
        logger.info("RabbitMQ clients closed")


app = FastAPI(lifespan=lifespan)

# Include routers
app.include_router(api_router)

# CORS Configuration
origins = settings.ORIGINS.split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def read_root():
    return {"message": "Data collector service is running"}
