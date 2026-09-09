from __future__ import annotations

import asyncio

import h2.config
import h2.connection
import h2.errors
import h2.events
import pytest

from hypercorn.app_wrappers import ASGIWrapper
from hypercorn.asyncio.run import worker_serve
from hypercorn.config import Config
from hypercorn.typing import ASGIReceiveCallable, ASGISendCallable, Scope


async def _lifespan(receive: ASGIReceiveCallable, send: ASGISendCallable) -> None:
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


async def slow_framework(
    scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> None:
    if scope["type"] == "lifespan":
        await _lifespan(receive, send)
        return
    await asyncio.sleep(100)


async def streaming_framework(
    scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
) -> None:
    if scope["type"] == "lifespan":
        await _lifespan(receive, send)
        return
    # Bidirectional stream: respond immediately, then keep both directions open.
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"start", "more_body": True})
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


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


@pytest.mark.asyncio
async def test_graceful_timeout_cancels_active_h2_stream(
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = Config()
    config.bind = ["127.0.0.1:0"]
    config.graceful_timeout = 0.5
    sockets = config.create_sockets()
    port = sockets.insecure_sockets[0].getsockname()[1]

    shutdown = asyncio.Event()
    serving = asyncio.ensure_future(
        worker_serve(
            ASGIWrapper(streaming_framework),
            config,
            sockets=sockets,
            shutdown_trigger=shutdown.wait,
        )
    )
    await asyncio.sleep(0.1)

    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    client = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
    client.initiate_connection()
    client.send_headers(
        1,
        [(":method", "POST"), (":path", "/"), (":scheme", "http"), (":authority", "hypercorn")],
    )
    client.send_data(1, b"start")  # No end_stream, the request stays open
    writer.write(client.data_to_send())
    await writer.drain()
    await asyncio.sleep(0.1)
    client.receive_data(await reader.read(65536))
    writer.write(client.data_to_send())
    await writer.drain()

    shutdown.set()
    try:
        # The stream never completes, so it is cancelled when graceful_timeout
        # expires. This must not raise out of the worker.
        await asyncio.wait_for(serving, timeout=5)

        data = b""
        while chunk := await asyncio.wait_for(reader.read(65536), timeout=1):
            data += chunk
        events = client.receive_data(data)
        assert isinstance(events[-1], h2.events.ConnectionTerminated)
        assert events[-1].error_code == h2.errors.ErrorCodes.NO_ERROR
        assert "Unhandled exception" not in caplog.text
    finally:
        writer.close()
