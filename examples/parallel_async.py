from __future__ import annotations

"""协程并行示例。

这个示例用 asyncio.sleep 模拟三个异步 IO 任务。它们从同一个 source 分发出去，
由 runtime 的共享 asyncio loop 并发执行。
"""

import asyncio
import threading
import time

from node_flow import AsyncFunctionNode, Event, FunctionNode, WorkflowRuntime


def make_async_node(name: str, delay: float):
    async def process(event: Event):
        start = time.perf_counter()
        print(f"[{name}] start payload={event.payload}")
        await asyncio.sleep(delay)
        elapsed = time.perf_counter() - start
        print(f"[{name}] done elapsed={elapsed:.2f}s")
        return {
            "branch": name,
            "input": event.payload,
            "elapsed": round(elapsed, 2),
        }

    return process


def main() -> None:
    runtime = WorkflowRuntime(max_workers=4)
    done = threading.Event()
    results = []

    source = runtime.register(FunctionNode("source", lambda event: event.payload))
    async_a = runtime.register(AsyncFunctionNode("async_a", make_async_node("async_a", 1.0)))
    async_b = runtime.register(AsyncFunctionNode("async_b", make_async_node("async_b", 1.0)))
    async_c = runtime.register(AsyncFunctionNode("async_c", make_async_node("async_c", 1.0)))

    def sink_process(event: Event):
        results.append(event.payload)
        print(f"[sink] got {event.payload}")
        if len(results) == 3:
            done.set()
        return None

    sink = runtime.register(FunctionNode("sink", sink_process))

    runtime.connect(source, async_a)
    runtime.connect(source, async_b)
    runtime.connect(source, async_c)
    runtime.connect(async_a, sink)
    runtime.connect(async_b, sink)
    runtime.connect(async_c, sink)

    started_at = time.perf_counter()
    runtime.start()
    try:
        runtime.trigger(source, {"request_id": "async-demo"})
        runtime.wait_until(done)
    finally:
        runtime.stop()

    total = time.perf_counter() - started_at
    print(f"total elapsed={total:.2f}s")


if __name__ == "__main__":
    main()
