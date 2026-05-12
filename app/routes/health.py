from fastapi import APIRouter, HTTPException
from dependencies.rabbitmq import data_mq_client, services_mq_client

router = APIRouter()


@router.get("/")
async def health_check():
    data_ok = await data_mq_client.health_probe()
    services_ok = await services_mq_client.health_probe()
    if data_ok and services_ok:
        return {"status": "ok", "data_mq": True, "services_mq": True}
    raise HTTPException(
        status_code=503,
        detail={"data_mq": data_ok, "services_mq": services_ok},
    )
