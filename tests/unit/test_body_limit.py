"""ASGI request limits, including missing or misleading Content-Length."""

import unittest
from unittest.mock import AsyncMock

from training_job_queue.body_limit import BodyLimitMiddleware


class BodyLimitTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, chunks, headers=(), limit=8):
        messages = [{"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
                    for i, chunk in enumerate(chunks)]
        receive = AsyncMock(side_effect=messages)
        send = AsyncMock()
        observed = []

        async def endpoint(scope, receive, send):
            observed.append(await receive())

        await BodyLimitMiddleware(endpoint, limit)(
            {"type": "http", "headers": list(headers)}, receive, send)
        return observed, send, receive

    async def test_exact_limit_is_forwarded(self):
        observed, send, _ = await self.exercise([b"12345678"], [(b"content-length", b"8")])
        self.assertEqual(observed, [{"type": "http.request", "body": b"12345678", "more_body": False}])
        send.assert_not_called()

    async def test_large_declared_length_is_rejected_before_reading(self):
        observed, send, receive = await self.exercise([b""], [(b"content-length", b"9")])
        self.assertEqual(observed, [])
        self.assertEqual(send.call_args_list[0].args[0]["status"], 413)
        receive.assert_not_called()

    async def test_actual_size_is_enforced_without_header(self):
        observed, send, _ = await self.exercise([b"123456789"])
        self.assertEqual(observed, [])
        self.assertEqual(send.call_args_list[0].args[0]["status"], 413)

    async def test_chunks_are_counted_together_even_with_small_declared_length(self):
        observed, send, _ = await self.exercise([b"1234", b"56789"], [(b"content-length", b"1")])
        self.assertEqual(observed, [])
        self.assertEqual(send.call_args_list[0].args[0]["status"], 413)

    async def test_small_chunks_are_replayed_as_one_body(self):
        observed, send, _ = await self.exercise([b"12", b"34", b"56"])
        self.assertEqual(observed[0]["body"], b"123456")
        send.assert_not_called()

    async def test_disconnect_does_not_execute_endpoint(self):
        endpoint = AsyncMock()
        receive = AsyncMock(return_value={"type": "http.disconnect"})
        send = AsyncMock()
        await BodyLimitMiddleware(endpoint)({"type": "http"}, receive, send)
        endpoint.assert_not_called()
        send.assert_not_called()

    async def test_non_http_scope_is_forwarded(self):
        endpoint, receive, send = AsyncMock(), AsyncMock(), AsyncMock()
        scope = {"type": "lifespan"}
        await BodyLimitMiddleware(endpoint)(scope, receive, send)
        endpoint.assert_awaited_once_with(scope, receive, send)
