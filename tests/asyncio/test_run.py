from __future__ import annotations

import asyncio

import pytest

from hypercorn.app_wrappers import ASGIWrapper
from hypercorn.asyncio.run import worker_serve
from hypercorn.config import Config
from hypercorn.typing import ASGIReceiveCallable, ASGISendCallable, Scope


async def slow_framework(
    scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await send({"type": "lifespan.shutdown.complete"})
                return
    await asyncio.sleep(100)


@pytest.mark.asyncio
async def test_graceful_timeout_bounds_active_connections() -> None:
    config = Config()
    config.bind = ["127.0.0.1:0"]
    config.graceful_timeout = 0.5
    sockets = config.create_sockets()
    port = sockets.insecure_sockets[0].getsockname()[1]

    shutdown = asyncio.Event()
    serving = asyncio.ensure_future(
        worker_serve(
            ASGIWrapper(slow_framework), config, sockets=sockets, shutdown_trigger=shutdown.wait
        )
    )
    await asyncio.sleep(0.1)

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET / HTTP/1.1\r\nHost: hypercorn\r\n\r\n")
    await writer.drain()
    await asyncio.sleep(0.1)

    shutdown.set()
    try:
        # The request never completes, so the drain must be bounded by
        # graceful_timeout rather than waiting for the connection to close.
        await asyncio.wait_for(serving, timeout=5)
    finally:
        writer.close()
