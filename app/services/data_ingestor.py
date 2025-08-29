import json
import aio_pika
from dependencies.rabbitmq import data_consumer, services_mq_client
from services.cache_service import CacheService
from services.validation_service import ValidationService
from config import settings
import logging

logger = logging.getLogger(__name__)

cache_service = CacheService()
validation_service = ValidationService()

@data_consumer(settings.DATA_QUEUE)
async def handle_data_message(message: aio_pika.abc.AbstractIncomingMessage):
    """Handle incoming raw data messages from wrappers"""
    try:
        # Parse raw message
        raw_data = json.loads(message.body.decode())
        
        # Validate message
        validated_message, validation_error = await validation_service.validate_message(raw_data)
        
        if validation_error:
            # Send validation error to services-mq
            await services_mq_client.publish(
                settings.VALIDATION_ERROR_QUEUE, 
                validation_error.model_dump_json()
            )
            logger.warning(f"Validation error for wrapper {validation_error.wrapper_id}: {validation_error.error_message}")
            await message.ack()
            return
        
        # Store validated message in cache
        await cache_service.store_message(validated_message)
        
        # Forward validated message to services queue
        await services_mq_client.publish(
            settings.COLLECTED_DATA_QUEUE, 
            validated_message.model_dump_json()
        )
        
        logger.info(f"Message validated and forwarded: wrapper_id={validated_message.wrapper_id}")
        await message.ack()
        
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        await message.reject(requeue=False)