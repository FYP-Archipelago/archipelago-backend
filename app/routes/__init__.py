from fastapi import APIRouter

from app.routes import auth, events, health, runs

router = APIRouter()
router.include_router(auth.router)
router.include_router(runs.router)
router.include_router(events.router)
router.include_router(health.router)
