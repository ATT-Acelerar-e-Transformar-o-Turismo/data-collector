import aio_pika
from fastapi import Depends
from utils.config import Config
from typing import AsyncGenerator

async def get_data_mq_connection() -> AsyncGenerator[aio_pika.Connection, None]:
    connection = await aio_pika.connect_robust(
        f"amqp://{Config.DATA_MQ_USER}:{Config.DATA_MQ_PASS}@{Config.DATA_MQ_HOST}:{Config.DATA_MQ_PORT}/",
        timeout=30,
        reconnect_interval=5,
        connection_attempts=10
    )
    try:
        yield connection
    except Exception as e:
        if not connection.is_closed:
            await connection.close()
        raise e

async def get_services_mq_connection() -> AsyncGenerator[aio_pika.Connection, None]:
    connection = await aio_pika.connect_robust(
        f"amqp://{Config.SERVICES_MQ_USER}:{Config.SERVICES_MQ_PASS}@{Config.SERVICES_MQ_HOST}:{Config.SERVICES_MQ_PORT}/",
        timeout=30,
        reconnect_interval=5,
        connection_attempts=10
    )
    try:
        yield connection
    except Exception as e:
        if not connection.is_closed:
            await connection.close()
        raise e 