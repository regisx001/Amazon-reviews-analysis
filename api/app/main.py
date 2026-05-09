from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .core.config import get_settings
from .api.v1 import analytics, predictions, stream, monitoring
from .routers import pages # Keeping pages for now, or I can move it too

def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Amazon Reviews Analytics API",
        description="Scalable API for Amazon customer review sentiment analysis",
        version="1.0.0"
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.frontend_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Static files
    app.mount("/static", StaticFiles(directory=settings.static_dir), name="static")

    # API v1 Routers (Mounted under /api for frontend compatibility)
    app.include_router(analytics.router,   prefix="/api")
    app.include_router(predictions.router, prefix="/api")
    app.include_router(stream.router,      prefix="/api")
    app.include_router(monitoring.router,  prefix="/api")
    
    # Page Routers
    app.include_router(pages.router)

    return app

app = create_app()
