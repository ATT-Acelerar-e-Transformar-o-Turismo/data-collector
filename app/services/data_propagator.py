import aio_pika
from aio_pika.abc import AbstractConnection, AbstractChannel
from schemas.message import DataMessage
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class DataPropagator:
    def __init__(self, connection: AbstractConnection):
        self.connection = connection
        self.channel: Optional[AbstractChannel] = None
        self._is_initialized = False

    async def initialize(self, mq_config: dict, max_retries: int = 5, retry_delay: int = 5):
        if self._is_initialized:
            logger.warning("DataPropagator already initialized")
            return

        # Add initial delay to ensure RabbitMQ is fully ready
        await asyncio.sleep(5)

        for attempt in range(max_retries):
            try:
                if self.connection.is_closed:
                    logger.error("Connection is closed, attempting to reconnect...")
                    await self.connection.connect()
                
                if not self.channel or self.channel.is_closed:
                    self.channel = await self.connection.channel()
                    self._is_initialized = True
                    logger.info("Successfully initialized channel")
                    return
                else:
                    logger.info("Channel already initialized")
                    return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Failed to initialize channel after {max_retries} attempts: {str(e)}")
                    raise
                logger.warning(f"Failed to initialize channel (attempt {attempt + 1}/{max_retries}): {str(e)}")
                await asyncio.sleep(retry_delay)

    async def propagate_message(self, message: DataMessage):
        if not self._is_initialized or not self.channel:
            logger.error("Cannot propagate message: channel not initialized")
            raise RuntimeError("Channel not initialized")

        try:
            services_queue = await self.channel.declare_queue(
                "services_queue",
                durable=True
            )
            
            await self.channel.default_exchange.publish(
                aio_pika.Message(
                    body=message.model_dump_json().encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=services_queue.name
            )
            logger.info(f"Message propagated to services queue: {message}")
        except Exception as e:
            logger.error(f"Error propagating message: {str(e)}")
            raise

    async def close(self):
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                logger.info("Channel closed successfully")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                logger.info("Connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing connections: {str(e)}")
            raise
        finally:
            self._is_initialized = False
            self.channel = None

    @classmethod
    async def create(cls, connection: aio_pika.Connection):
        return cls(connection) 