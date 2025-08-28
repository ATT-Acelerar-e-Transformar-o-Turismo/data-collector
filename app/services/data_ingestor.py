import json
import aio_pika
from schemas.message import DataMessage
from dependencies.rabbitmq import data_consumer, services_mq_client
from services.cache_service import CacheService
from config import settings
import logging

logger = logging.getLogger(__name__)

cache_service = CacheService()

@data_consumer(settings.DATA_QUEUE)
async def handle_data_message(message: aio_pika.abc.AbstractIncomingMessage):
    """Handle incoming data messages from data-mq"""
    try:
        # Parse and validate message
        data = json.loads(message.body.decode())
        message_obj = DataMessage(**data)
        
        # Store message in cache
        await cache_service.store_message(message_obj)
        
        # Forward message to services queue
        await services_mq_client.publish(settings.COLLECTED_DATA_QUEUE, message.body)
        
        logger.info(f"Message processed successfully: {message_obj.resource_id}")
        await message.ack()
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        await message.reject(requeue=False)
