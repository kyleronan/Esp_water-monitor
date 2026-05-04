"""Fixtures router — Phase 2 placeholder."""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/fixtures")


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def fixtures_page(request: Request):
    return request.app.state.templates.TemplateResponse("fixtures.html", {
        "request": request,
        "page": "fixtures",
    })
