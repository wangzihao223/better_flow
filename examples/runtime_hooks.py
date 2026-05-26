from __future__ import annotations

"""Show how to observe WorkflowRuntime with hooks."""

import threading
import time

from node_flow import Event, FunctionNode, WorkflowRuntime


def prepare(event: Event):
    return {"value": event.payload["value"] + 1}


def work(event: Event):
    time.sleep(0.2)
    return {"value": event.payload["value"] * 10}


def main() -> None:
    runtime = WorkflowRuntime(max_workers=2)
    done = threading.Event()

    def hook(kind: str, data: dict) -> None:
        if kind in {
            "runtime_started",
            "event_created",
            "event_dispatched",
            "task_submitted",
            "node_started",
            "node_finished",
            "task_done",
            "runtime_stopped",
        }:
            print(f"[hook] {kind}: {data}")

    runtime.add_hook(hook)

    start = runtime.register(FunctionNode("start", prepare))
    worker = runtime.register(FunctionNode("worker", work))

    def sink_process(event: Event):
        print(f"[sink] payload={event.payload}")
        done.set()
        return None

    sink = runtime.register(FunctionNode("sink", sink_process))

    runtime.connect(start, worker)
    runtime.connect(worker, sink)

    runtime.start()
    try:
        runtime.trigger(start, {"value": 1})
        runtime.wait_until(done, timeout=3)
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
