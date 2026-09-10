from __future__ import annotations

import asyncio
from typing import Any

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


class CancellationSwallowingFramework:
    # Some frameworks catch the cancellation and carry on, e.g. restate_sdk's
    # ServerInvocationContext.enter() does `except asyncio.CancelledError: pass`
    # and then keeps awaiting the request. The server must still exit.
    # `release` lets the test unwind the stuck request afterwards; otherwise a
    # failing run also hangs the event loop teardown.

    def __init__(self) -> None:
        self.release = asyncio.Event()

    async def __call__(
        self, scope: Scope, receive: ASGIReceiveCallable, send: ASGISendCallable
    ) -> None:
        if scope["type"] == "lifespan":
            await _lifespan(receive, send)
            return
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            pass
        await self.release.wait()


H2_REQUEST_HEADERS = [
    (":method", "POST"),
    (":path", "/"),
    (":scheme", "http"),
    (":authority", "hypercorn"),
]


async def _serve(
    framework: Any, graceful_timeout: float
) -> tuple[asyncio.Future, asyncio.Event, int]:
    config = Config()
    config.bind = ["127.0.0.1:0"]
    config.graceful_timeout = graceful_timeout
    sockets = config.create_sockets()
    port = sockets.insecure_sockets[0].getsockname()[1]
    shutdown = asyncio.Event()
    serving = asyncio.ensure_future(
        worker_serve(
            ASGIWrapper(framework), config, sockets=sockets, shutdown_trigger=shutdown.wait
        )
    )
    await asyncio.sleep(0.1)
    return serving, shutdown, port


async def _open_h2_stream(
    port: int,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter, h2.connection.H2Connection]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    client = h2.connection.H2Connection(h2.config.H2Configuration(client_side=True))
    client.initiate_connection()
    client.send_headers(1, H2_REQUEST_HEADERS)
    client.send_data(1, b"start")  # No end_stream, the request stays open
    writer.write(client.data_to_send())
    await writer.drain()
    await asyncio.sleep(0.1)
    client.receive_data(await reader.read(65536))
    writer.write(client.data_to_send())
    await writer.drain()
    return reader, writer, client


@pytest.mark.asyncio
async def test_graceful_timeout_bounds_app_that_swallows_cancellation() -> None:
    framework = CancellationSwallowingFramework()
    serving, shutdown, port = await _serve(framework, 0.5)
    reader, writer, client = await _open_h2_stream(port)

    shutdown.set()
    try:
        # Cancelling the connection tasks is not enough on its own: the app
        # ignores the first cancellation, so waiting for the cancellation to
        # complete hangs. The drain must be bounded regardless of the app.
        # asyncio.wait rather than wait_for: wait_for would cancel `serving`
        # and then hang on that cancellation for the same reason.
        done, _ = await asyncio.wait({serving}, timeout=5)
        assert serving in done, "worker_serve did not exit after graceful_timeout"
        serving.result()
    finally:
        framework.release.set()
        writer.close()
        await asyncio.wait({serving}, timeout=5)


@pytest.mark.asyncio
async def test_request_during_drain_does_not_crash_worker(
    caplog: pytest.LogCaptureFixture,
) -> None:
    serving, shutdown, port = await _serve(streaming_framework, 2)
    reader, writer, client = await _open_h2_stream(port)

    shutdown.set()
    await asyncio.sleep(0.1)
    # A client that has not seen a GOAWAY yet starts a new request. HEADERS
    # and DATA arrive in one read. The terminated worker refuses the stream
    # with RST_STREAM and must ignore the DATA that follows it. It must not
    # let the connection's exception escape worker_serve: a non-zero worker
    # exit makes the master shut down every other worker.
    client.send_headers(3, H2_REQUEST_HEADERS)
    client.send_data(3, b"body")
    writer.write(client.data_to_send())
    await writer.drain()

    try:
        done, _ = await asyncio.wait({serving}, timeout=5)
        assert serving in done, "worker_serve did not exit after graceful_timeout"
        serving.result()
        assert "Unhandled exception" not in caplog.text
    finally:
        writer.close()
