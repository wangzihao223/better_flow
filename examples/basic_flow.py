from __future__ import annotations

"""node_flow 基础示例。

这个示例展示：
- TimerSource 定时发事件
- RouterNode 做简单分流
- FunctionNode 处理事件
- PrintSink 打印结果
- WorkflowRuntime 统一调度执行
"""

import time

from node_flow import Event, FunctionNode, PrintSink, RouterNode, TimerSource, WorkflowRuntime


def route(event: Event) -> str:
    """根据 count 的奇偶决定走 fast 还是 slow。"""
    if event.payload["count"] % 2 == 0:
        return "fast"
    return "slow"


def fast_process(event: Event):
    return {
        "branch": "fast",
        "count": event.payload["count"],
        "value": event.payload["count"] * 10,
    }


def slow_process(event: Event):
    time.sleep(0.2)
    return {
        "branch": "slow",
        "count": event.payload["count"],
        "value": event.payload["count"] * 100,
    }


def main() -> None:
    runtime = WorkflowRuntime(max_workers=4, max_cpu_workers=1)

    timer = runtime.register(TimerSource("timer", interval=0.5, count_limit=6))
    router = runtime.register(RouterNode("router", route))
    fast = runtime.register(FunctionNode("fast", fast_process))
    slow = runtime.register(FunctionNode("slow", slow_process))
    sink = runtime.register(PrintSink("sink"))

    runtime.connect(timer, router)
    runtime.connect(router, fast)
    runtime.connect(router, slow)
    runtime.connect(fast, sink)
    runtime.connect(slow, sink)

    runtime.start()
    try:
        time.sleep(4)
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
