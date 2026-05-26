from __future__ import annotations

"""WorkflowRuntime 的核心调度语义测试。

这里先覆盖入口行为：trigger 会执行指定节点自己，emit 只从 source 的
下游开始传播事件。
"""

import threading
import unittest

from node_flow import Event, FunctionNode, WorkflowRuntime


class RuntimeEntryTests(unittest.TestCase):
    def test_trigger_executes_target_node_and_downstream(self) -> None:
        """trigger 会执行目标节点的 process，然后继续向下游传播。"""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        def start_fn(event: Event):
            calls.append(("start", dict(event.payload)))
            return {"value": event.payload["value"] + 1}

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            done.set()
            return None

        start = runtime.register(FunctionNode("start", start_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(start, sink)

        runtime.start()
        try:
            runtime.trigger(start, {"value": 1})
            self.assertTrue(done.wait(2), "trigger did not reach downstream node")
        finally:
            runtime.stop()

        self.assertEqual(
            calls,
            [
                ("start", {"value": 1}),
                ("sink", {"value": 2}),
            ],
        )

    def test_emit_skips_source_node_and_dispatches_downstream(self) -> None:
        """emit 不执行 source 自己，只把事件发给 source 的下游节点。"""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        def source_fn(event: Event):
            calls.append(("source", dict(event.payload)))
            return {"value": 999}

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            done.set()
            return None

        source = runtime.register(FunctionNode("source", source_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(source, sink)

        runtime.start()
        try:
            runtime.emit(source, {"value": 1})
            self.assertTrue(done.wait(2), "emit did not reach downstream node")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 1})])


if __name__ == "__main__":
    unittest.main()
