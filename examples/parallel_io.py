from __future__ import annotations

"""IO 并行示例。

这个示例用 time.sleep 模拟三个阻塞 IO 任务。它们从同一个 source 分发出去，
由 runtime 的线程池并行执行，最后都进入同一个 sink。
"""

import threading
import time

from node_flow import Event, FunctionNode, WorkflowRuntime


def make_io_node(name: str, delay: float):
    def process(event: Event):
        start = time.perf_counter()
        print(f"[{name}] start payload={event.payload}")
        time.sleep(delay)
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
    io_a = runtime.register(FunctionNode("io_a", make_io_node("io_a", 1.0)))
    io_b = runtime.register(FunctionNode("io_b", make_io_node("io_b", 1.0)))
    io_c = runtime.register(FunctionNode("io_c", make_io_node("io_c", 1.0)))

    def sink_process(event: Event):
        results.append(event.payload)
        print(f"[sink] got {event.payload}")
        if len(results) == 3:
            done.set()
        return None

    sink = runtime.register(FunctionNode("sink", sink_process))

    runtime.connect(source, io_a)
    runtime.connect(source, io_b)
    runtime.connect(source, io_c)
    runtime.connect(io_a, sink)
    runtime.connect(io_b, sink)
    runtime.connect(io_c, sink)

    started_at = time.perf_counter()
    runtime.start()
    try:
        runtime.trigger(source, {"request_id": "demo"})
        runtime.wait_until(done)
    finally:
        runtime.stop()

    total = time.perf_counter() - started_at
    print(f"total elapsed={total:.2f}s")


if __name__ == "__main__":
    main()
