from __future__ import annotations

"""演示入口。

这个脚本展示最小节点图如何被 runtime 统一拉起和调度。
"""

import time

from .event import Event
from .nodes import FunctionNode, PrintSink, TimerSource
from .runtime import WorkflowRuntime


def delay(seconds: float):
    # 这里用一个普通同步函数模拟阻塞 IO。
    def run(event: Event):
        time.sleep(seconds)
        print(f"[delay {seconds}s] -> {event.payload}")
        return event.payload

    return run


def main() -> None:
    runtime = WorkflowRuntime(max_workers=8)

    timer = runtime.register(TimerSource(interval=0.5, count_limit=4))
    fast = runtime.register(FunctionNode("fast_branch", delay(0.2)))
    slow = runtime.register(FunctionNode("slow_branch", delay(1.0)))
    sink = runtime.register(PrintSink("sink"))

    runtime.connect(timer, fast)
    runtime.connect(timer, slow)
    runtime.connect(fast, sink)
    runtime.connect(slow, sink)

    runtime.start()
    time.sleep(4)
    runtime.stop()


if __name__ == "__main__":
    main()
