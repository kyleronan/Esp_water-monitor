"""Strong references for fire-and-forget asyncio tasks.

asyncio keeps only a WEAK reference to a running task. A bare
``asyncio.create_task(...)`` / ``asyncio.ensure_future(...)`` whose return
value is discarded can therefore be garbage-collected mid-flight: the
coroutine simply stops, silently, with no traceback and no log line. On this
add-on that trap has real teeth — the dispatch sites are a leak-test start, a
low-pressure alert, a pump-fault alert and a supply-regime-shift banner.

``leak_test_scheduler._check_and_run`` already worked around this with a local
closure plus a per-instance set, and the comment there calls the bare form
"a known antipattern". This module is that pattern factored out so the four
remaining sites can use it, plus a guard so a crashing background task is
LOGGED instead of vanishing into a never-retrieved exception.

Enforcement: ruff ``RUF006`` (see ruff.toml) flags any new bare dispatch.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Optional, Set

log = logging.getLogger(__name__)

#: Module-level so the reference survives the caller's frame. Tasks remove
#: themselves on completion (add_done_callback below), so this set is bounded
#: by the number of IN-FLIGHT background tasks, not by how many ever ran.
_TASKS: Set["asyncio.Task"] = set()


def spawn(coro: Awaitable[Any], *, name: str) -> Optional["asyncio.Task"]:
    """Schedule ``coro`` as a background task and keep it alive.

    Returns the task, or ``None`` when there is no running loop (sync unit
    tests reach some of these call sites). The coroutine is closed in that
    case so Python does not warn about it never being awaited.

    Exceptions other than ``CancelledError`` are logged and swallowed: every
    caller is fire-and-forget, and an unretrieved exception on a task nobody
    holds is only visible at GC time, in a destructor warning.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        return None

    task = asyncio.create_task(_guard(coro, name), name=name)  # noqa: RUF006
    _TASKS.add(task)
    task.add_done_callback(_TASKS.discard)
    return task


async def _guard(coro: Awaitable[Any], name: str) -> Any:
    try:
        return await coro
    except asyncio.CancelledError:
        raise
    except Exception:
        log.error("background task %r failed", name, exc_info=True)
        return None


def pending_count() -> int:
    """How many spawned tasks are still in flight (tests / diagnostics)."""
    return len(_TASKS)
