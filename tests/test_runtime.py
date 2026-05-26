from __future__ import annotations

"""WorkflowRuntime 的核心调度语义测试。

这里先覆盖入口行为：trigger 会执行指定节点自己，emit 只从 source 的
下游开始传播事件。
"""

import threading
import unittest

from node_flow import Event, FunctionNode, RouterNode, WorkflowRuntime


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

    def test_router_dispatches_only_selected_branch_once(self) -> None:
        """RouterNode filters one hop and lets the selected branch continue."""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=4)

        router = runtime.register(RouterNode("router", lambda event: "image"))

        def image_fn(event: Event):
            calls.append(("image", dict(event.payload)))
            return event.payload

        def text_fn(event: Event):
            calls.append(("text", dict(event.payload)))
            return event.payload

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            done.set()
            return None

        image = runtime.register(FunctionNode("image", image_fn))
        text = runtime.register(FunctionNode("text", text_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))

        runtime.connect(router, image)
        runtime.connect(router, text)
        runtime.connect(image, sink)

        runtime.start()
        try:
            runtime.trigger(router, {"kind": "image"})
            self.assertTrue(done.wait(2), "selected branch did not reach sink")
        finally:
            runtime.stop()

        self.assertEqual(
            calls,
            [
                ("image", {"kind": "image"}),
                ("sink", {"kind": "image"}),
            ],
        )

    def test_router_empty_route_stops_propagation(self) -> None:
        """RouterNode returning an empty list means no downstream branch is selected."""
        calls = []
        runtime = WorkflowRuntime(max_workers=2)

        router = runtime.register(RouterNode("router", lambda event: []))

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(router, sink)

        runtime.start()
        try:
            runtime.trigger(router, {"kind": "none"})
            threading.Event().wait(0.1)
        finally:
            runtime.stop()

        self.assertEqual(calls, [])

    def test_sync_node_error_is_recorded_and_stops_branch(self) -> None:
        """A failing sync node records an error event and does not call downstream."""
        calls = []
        runtime = WorkflowRuntime(max_workers=2)

        def bad_fn(event: Event):
            raise ValueError("bad payload")

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            return None

        bad = runtime.register(FunctionNode("bad", bad_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(bad, sink)

        runtime.start()
        try:
            runtime.trigger(bad, {"value": 1})
            threading.Event().wait(0.1)
        finally:
            runtime.stop()

        self.assertEqual(calls, [])
        self.assertEqual(len(runtime.errors), 1)

        error = runtime.errors[0]
        self.assertEqual(error.source, "bad")
        self.assertEqual(error.name, "error")
        self.assertEqual(error.payload["node_id"], "bad")
        self.assertEqual(error.payload["error_type"], "ValueError")
        self.assertEqual(error.payload["error_message"], "bad payload")
        self.assertEqual(error.payload["payload"], {"value": 1})


if __name__ == "__main__":
    unittest.main()
