from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    ORIGINS: str = Field(default="localhost", env="ORIGINS")
    
    # RabbitMQ Configuration
    DATA_RABBITMQ_URL: str = Field(
        default="amqp://user:password@data-mq:5672/", env="DATA_RABBITMQ_URL"
    )
    SERVICES_RABBITMQ_URL: str = Field(
        default="amqp://user:password@services-mq:5672/", env="SERVICES_RABBITMQ_URL"
    )
    
    # Queue Configuration
    DATA_QUEUE: str = Field(default="data", env="DATA_QUEUE")
    COLLECTED_DATA_QUEUE: str = Field(default="collected_data", env="COLLECTED_DATA_QUEUE")
    VALIDATION_ERROR_QUEUE: str = Field(default="validation_errors", env="VALIDATION_ERROR_QUEUE")

    # Redis Configuration
    REDIS_URL: str = Field(default="redis://redis:6379", env="REDIS_URL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings() 