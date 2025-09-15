from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from config import settings
from dependencies.rabbitmq import data_mq_client, services_mq_client
from routes import router as api_router
import logging
from contextlib import asynccontextmanager

# Import services to register consumers
import services.data_ingestor

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await data_mq_client.connect()
        await services_mq_client.connect()
        await data_mq_client.start_consumers()
        await services_mq_client.start_consumers()
        logger.info("RabbitMQ clients initialized successfully")
        yield
    finally:
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