from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from ..core.config import get_settings
from ..templates import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    settings = get_settings()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"target_product": settings.target_product},
    )
