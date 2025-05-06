import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Data MQ Configuration
    DATA_MQ_HOST = os.getenv('DATA_MQ_HOST', 'data-mq')
    DATA_MQ_PORT = int(os.getenv('DATA_MQ_PORT', '5672'))
    DATA_MQ_USER = os.getenv('DATA_MQ_USER', 'user')
    DATA_MQ_PASS = os.getenv('DATA_MQ_PASS', 'password')

    # Services MQ Configuration
    SERVICES_MQ_HOST = os.getenv('SERVICES_MQ_HOST', 'services-mq')
    SERVICES_MQ_PORT = int(os.getenv('SERVICES_MQ_PORT', '5672'))
    SERVICES_MQ_USER = os.getenv('SERVICES_MQ_USER', 'user')
    SERVICES_MQ_PASS = os.getenv('SERVICES_MQ_PASS', 'password')

    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))

    @classmethod
    def get_data_mq_config(cls) -> dict:
        return {
            'host': cls.DATA_MQ_HOST,
            'port': cls.DATA_MQ_PORT,
            'user': cls.DATA_MQ_USER,
            'password': cls.DATA_MQ_PASS
        }

    @classmethod
    def get_services_mq_config(cls) -> dict:
        return {
            'host': cls.SERVICES_MQ_HOST,
            'port': cls.SERVICES_MQ_PORT,
            'user': cls.SERVICES_MQ_USER,
            'password': cls.SERVICES_MQ_PASS
        } 