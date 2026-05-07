from fastapi.templating import Jinja2Templates

from .core.config import get_settings

settings = get_settings()
templates = Jinja2Templates(directory=settings.templates_dir)
