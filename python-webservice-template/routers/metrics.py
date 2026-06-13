from fastapi import APIRouter, Response

from core.dependencies import HealthServiceDep, MetricsServiceDep

router = APIRouter()


@router.get("/metrics")
async def get_metrics(health_service: HealthServiceDep, metrics_service: MetricsServiceDep):
    await metrics_service.refresh(health_service)
    body, content_type = metrics_service.render()
    return Response(content=body, media_type=content_type)
