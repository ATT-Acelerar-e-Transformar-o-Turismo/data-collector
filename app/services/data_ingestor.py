import json
import aio_pika
from aio_pika.abc import AbstractChannel
from schemas.message import DataMessage
from typing import Callable, Awaitable
from dependencies.rabbitmq import get_data_mq_connection
from services.cache_service import CacheService
from fastapi import Depends
import asyncio

class DataIngestor:
    def __init__(self, connection: aio_pika.Connection, cache_service: CacheService):
        self.connection = connection
        self.channel: AbstractChannel = None
        self.on_message_callback: Callable[[DataMessage], Awaitable[None]] = None
        self.cache_service = cache_service

    async def initialize(self, on_message_callback: Callable[[DataMessage], Awaitable[None]]):
        self.on_message_callback = on_message_callback
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=1)

    async def consume_queue(self):
        queue = await self.channel.declare_queue(
            "data_queue",
            durable=True
        )

        try:
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        try:
                            # Parse and validate message
                            data = json.loads(message.body.decode())
                            message_obj = DataMessage(**data)
                            
                            # Store message in cache
                            await self.cache_service.store_message(message_obj)
                            
                            # Process message using callback
                            await self.on_message_callback(message_obj)
                            
                            print(f"Message ingested successfully: {message_obj}")
                        except Exception as e:
                            print(f"Error ingesting message: {str(e)}")
                            await message.nack(requeue=True)
        except asyncio.CancelledError:
            # Handle graceful shutdown
            print("Gracefully shutting down consumer...")
            if not self.channel.is_closed:
                await self.channel.close()
            raise
        except Exception as e:
            print(f"Error in consume_queue: {str(e)}")
            raise

    @classmethod
    async def create(cls, connection: aio_pika.Connection = Depends(get_data_mq_connection), cache_service: CacheService = Depends(CacheService.create)):
        return cls(connection, cache_service)

    async def close(self):
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                print("Channel closed successfully")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                print("Connection closed successfully")
        except Exception as e:
            print(f"Error closing connections: {str(e)}")
            raise 