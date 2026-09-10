from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from hypercorn.app_wrappers import ASGIWrapper
from hypercorn.asyncio.tcp_server import TCPServer
from hypercorn.asyncio.worker_context import WorkerContext
from hypercorn.config import Config
from hypercorn.events import Closed, RawData
from .helpers import MemoryReader, MemoryWriter
from ..helpers import echo_framework


class OSErrorOnDrainWriter(MemoryWriter):
    async def drain(self) -> None:
        raise OSError("connection_lost carried a non ConnectionError")


class OSErrorOnWaitClosedWriter(MemoryWriter):
    async def wait_closed(self) -> None:
        raise OSError("SSL shutdown timed out")


def _server(writer: MemoryWriter) -> TCPServer:
    return TCPServer(
        ASGIWrapper(echo_framework),
        asyncio.get_running_loop(),
        Config(),
        WorkerContext(None),
        {},
        MemoryReader(),  # type: ignore
        writer,  # type: ignore
    )


@pytest.mark.asyncio
async def test_protocol_send_closes_on_oserror() -> None:
    server = _server(OSErrorOnDrainWriter())
    server.protocol = AsyncMock()

    await server.protocol_send(RawData(data=b"data"))

    server.protocol.handle.assert_awaited_once_with(Closed())


@pytest.mark.asyncio
async def test_close_ignores_oserror() -> None:
    server = _server(OSErrorOnWaitClosedWriter())

    await server._close()


@pytest.mark.asyncio
async def test_completes_on_closed() -> None:
    event_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    server = TCPServer(
        ASGIWrapper(echo_framework),
        event_loop,
        Config(),
        WorkerContext(None),
        {},
        MemoryReader(),  # type: ignore
        MemoryWriter(),  # type: ignore
    )
    server.reader.close()  # type: ignore
    await server.run()
    # Key is that this line is reached, rather than the above line
    # hanging.


@pytest.mark.asyncio
async def test_complets_on_half_close() -> None:
    event_loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()

    server = TCPServer(
        ASGIWrapper(echo_framework),
        event_loop,
        Config(),
        WorkerContext(None),
        {},
        MemoryReader(),  # type: ignore
        MemoryWriter(),  # type: ignore
    )
    task = event_loop.create_task(server.run())
    await server.reader.send(b"GET / HTTP/1.1\r\nHost: hypercorn\r\n\r\n")  # type: ignore
    server.reader.close()  # type: ignore
    await task
    data = await server.writer.receive()  # type: ignore
    assert (
        data
        == b"HTTP/1.1 200 \r\ncontent-length: 348\r\ndate: Thu, 01 Jan 1970 01:23:20 GMT\r\nserver: hypercorn-h11\r\n\r\n"  # noqa: E501
    )
