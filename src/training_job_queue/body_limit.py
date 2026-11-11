"""Bound HTTP request bodies before JSON parsing or endpoint execution."""

from starlette.responses import JSONResponse

MAX_REQUEST_BODY_BYTES = 64 * 1024


class BodyLimitMiddleware:
    def __init__(self, app, max_bytes=MAX_REQUEST_BODY_BYTES):
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def reject():
            response = JSONResponse(status_code=413, content={
                "detail": f"Request body exceeds {self.max_bytes} bytes"})
            await response(scope, receive, send)

        for name, value in scope.get("headers", []):
            if name.lower() == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    continue  # Always enforce the actual received byte count.
                if declared > self.max_bytes:
                    await reject()
                    return

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > self.max_bytes:
                await reject()
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        delivered = False

        async def replay():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        await self.app(scope, replay, send)
