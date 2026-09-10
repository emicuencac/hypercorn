from __future__ import annotations

import asyncio

import pytest

from hypercorn.asyncio.task_group import TaskGroup
from hypercorn.asyncio.worker_context import AsyncioSingleTask


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["restart", "stop"])
async def test_single_task_propagates_caller_cancellation(method: str) -> None:
    event_loop = asyncio.get_running_loop()
    single_task = AsyncioSingleTask()
    cancelled = asyncio.Event()
    release = asyncio.Event()

    async def action() -> None:
        try:
            await asyncio.sleep(100)
        finally:
            cancelled.set()
            await release.wait()

    async with TaskGroup(event_loop) as task_group:
        await single_task.restart(task_group, action)
        if method == "restart":
            caller = event_loop.create_task(single_task.restart(task_group, action))
        else:
            caller = event_loop.create_task(single_task.stop())
        await cancelled.wait()
        caller.cancel()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await caller

        await single_task.stop()
