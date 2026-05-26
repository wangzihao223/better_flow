from __future__ import annotations

"""CPU 并行示例。

这个示例用素数计数模拟 CPU 密集任务。三个 CpuNode 会被提交到进程池，
适合观察 CPU 密集任务与普通线程池 IO 任务的区别。
"""

import math
import threading
import time

from node_flow import CpuNode, Event, FunctionNode, WorkflowRuntime


def is_prime(value: int) -> bool:
    if value < 2:
        return False
    if value == 2:
        return True
    if value % 2 == 0:
        return False

    limit = int(math.sqrt(value))
    for divisor in range(3, limit + 1, 2):
        if value % divisor == 0:
            return False
    return True


def count_primes(event: Event):
    limit = event.payload["limit"]
    started_at = time.perf_counter()
    total = sum(1 for value in range(limit) if is_prime(value))
    elapsed = time.perf_counter() - started_at
    return {
        "branch": event.payload["branch"],
        "limit": limit,
        "prime_count": total,
        "elapsed": round(elapsed, 2),
    }


def main() -> None:
    runtime = WorkflowRuntime(max_workers=4, max_cpu_workers=3)
    done = threading.Event()
    results = []

    source = runtime.register(FunctionNode("source", lambda event: event.payload))
    cpu_a = runtime.register(CpuNode("cpu_a", count_primes))
    cpu_b = runtime.register(CpuNode("cpu_b", count_primes))
    cpu_c = runtime.register(CpuNode("cpu_c", count_primes))

    def payload_for(branch: str, limit: int):
        def process(event: Event):
            return {
                "branch": branch,
                "limit": limit,
                "request_id": event.payload["request_id"],
            }

        return process

    prepare_a = runtime.register(
        FunctionNode("prepare_a", payload_for("cpu_a", 1000_000))
    )
    prepare_b = runtime.register(
        FunctionNode("prepare_b", payload_for("cpu_b", 2000_000))
    )
    prepare_c = runtime.register(
        FunctionNode("prepare_c", payload_for("cpu_c", 3000_000))
    )

    def sink_process(event: Event):
        results.append(event.payload)
        print(f"[sink] got {event.payload}")
        if len(results) == 3:
            done.set()
        return None

    sink = runtime.register(FunctionNode("sink", sink_process))

    runtime.connect(source, prepare_a)
    runtime.connect(source, prepare_b)
    runtime.connect(source, prepare_c)
    runtime.connect(prepare_a, cpu_a)
    runtime.connect(prepare_b, cpu_b)
    runtime.connect(prepare_c, cpu_c)
    runtime.connect(cpu_a, sink)
    runtime.connect(cpu_b, sink)
    runtime.connect(cpu_c, sink)

    started_at = time.perf_counter()
    runtime.start()
    try:
        print("[stats before]", runtime.stats())
        runtime.trigger(source, {"request_id": "cpu-demo"})
        print("[stats after trigger]", runtime.stats())
        runtime.wait_until(done)
        print("[stats completed]", runtime.stats())
    finally:
        runtime.stop()

    total = time.perf_counter() - started_at
    print(f"total elapsed={total:.2f}s")
    print("[stats stopped]", runtime.stats())


if __name__ == "__main__":
    main()
