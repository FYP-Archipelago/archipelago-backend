from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.dependencies import require_origin


class AuthRequestMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope["path"]
        is_auth = path == "/auth" or path.startswith("/auth/")
        is_run_related = path in ("/presets", "/runs") or path.startswith("/runs/")
        if not (is_auth or is_run_related):
            await self.app(scope, receive, send)
            return

        async def no_store(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message)["Cache-Control"] = "no-store"
            await send(message)

        try:
            if is_auth and scope["method"] == "POST":
                require_origin(Request(scope, receive))
            await self.app(scope, receive, no_store)
        except HTTPException as error:
            await JSONResponse({"detail": error.detail}, status_code=error.status_code)(scope, receive, no_store)
