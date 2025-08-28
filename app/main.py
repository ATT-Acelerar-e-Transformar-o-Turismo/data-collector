from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from services.data_ingestor import DataIngestor
from services.data_propagator import DataPropagator
from utils.config import Config
from dependencies.rabbitmq import get_data_mq_connection, get_services_mq_connection
from routes import router as api_router
import asyncio
import logging
from contextlib import asynccontextmanager
import os

logger = logging.getLogger(__name__)

# Global variables to store service instances
data_ingestor: DataIngestor = None
data_propagator: DataPropagator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global data_ingestor, data_propagator
    consumer_task = None
    try:
        # Initialize data propagator with retry
        max_retries = 10
        retry_delay = 10

        for attempt in range(max_retries):
            try:
                data_propagator = await get_data_propagator()
                await data_propagator.initialize(Config.get_services_mq_config())
                logger.info("Data propagator initialized successfully")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to initialize data propagator after {max_retries} attempts: {str(e)}"
                    )
                    raise
                logger.warning(
                    f"Failed to initialize data propagator (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                await asyncio.sleep(retry_delay)

        # Initialize data ingestor with retry
        for attempt in range(max_retries):
            try:
                data_ingestor = await get_data_ingestor()
                await data_ingestor.initialize(handle_ingested_message)
                logger.info("Data ingestor initialized successfully")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(
                        f"Failed to initialize data ingestor after {max_retries} attempts: {str(e)}"
                    )
                    raise
                logger.warning(
                    f"Failed to initialize data ingestor (attempt {attempt + 1}/{max_retries}): {str(e)}"
                )
                await asyncio.sleep(retry_delay)

        # Start consuming messages
        consumer_task = asyncio.create_task(data_ingestor.consume_queue())
        logger.info("Message consumption started")

        yield
    except Exception as e:
        logger.error(f"Failed to initialize services: {str(e)}")
        raise
    finally:
        # Cancel consumer task if it exists
        if consumer_task:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Error cancelling consumer task: {str(e)}")

        # Close data ingestor
        if data_ingestor:
            try:
                await data_ingestor.close()
                logger.info("Data ingestor closed successfully")
            except Exception as e:
                logger.error(f"Error closing data ingestor: {str(e)}")

        # Close data propagator
        if data_propagator:
            try:
                await data_propagator.close()
                logger.info("Data propagator closed successfully")
            except Exception as e:
                logger.error(f"Error closing data propagator: {str(e)}")


app = FastAPI(lifespan=lifespan)

# Include routers
app.include_router(api_router)


async def get_data_ingestor():
    connection = await anext(get_data_mq_connection())
    return await DataIngestor.create(connection)


async def get_data_propagator():
    connection = await anext(get_services_mq_connection())
    return await DataPropagator.create(connection)


async def handle_ingested_message(message):
    global data_propagator
    if data_propagator:
        await data_propagator.propagate_message(message)


# CORS Configuration
origins = os.getenv("ORIGINS", "localhost").split(",")
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