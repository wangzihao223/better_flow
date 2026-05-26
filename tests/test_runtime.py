from __future__ import annotations

"""WorkflowRuntime 的核心调度语义测试。

这里先覆盖入口行为：trigger 会执行指定节点自己，emit 只从 source 的
下游开始传播事件。
"""

import asyncio
import threading
import time
import unittest

from node_flow import (
    AsyncFunctionNode,
    CpuNode,
    Event,
    FilterNode,
    FunctionNode,
    RouterNode,
    TimerSource,
    WorkflowRuntime,
)


def cpu_add_one(event: Event):
    return {"value": event.payload["value"] + 1}


def cpu_fail(event: Event):
    raise RuntimeError("cpu failed")


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

    def test_async_node_result_reaches_downstream(self) -> None:
        """AsyncFunctionNode runs on the asyncio loop and propagates its result."""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        async def async_fn(event: Event):
            await asyncio.sleep(0.5)
            return {"value": event.payload["value"] + 1}

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            done.set()
            return None

        async_node = runtime.register(AsyncFunctionNode("async", async_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(async_node, sink)

        runtime.start()
        try:
            runtime.trigger(async_node, {"value": 1})
            self.assertTrue(done.wait(2), "async node did not reach sink")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 2})])

    def test_async_node_error_is_recorded(self) -> None:
        """AsyncFunctionNode errors are recorded and stop the branch."""
        calls = []
        runtime = WorkflowRuntime(max_workers=2)

        async def bad_async_fn(event: Event):
            raise RuntimeError("async failed")

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            return None

        bad = runtime.register(AsyncFunctionNode("bad_async", bad_async_fn))
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
        self.assertEqual(runtime.errors[0].payload["node_id"], "bad_async")
        self.assertEqual(runtime.errors[0].payload["error_type"], "RuntimeError")

    def test_cpu_node_result_reaches_downstream(self) -> None:
        """CpuNode runs in the process pool and propagates its result."""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2, max_cpu_workers=1)

        cpu = runtime.register(CpuNode("cpu", cpu_add_one))

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            done.set()
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(cpu, sink)

        runtime.start()
        try:
            runtime.trigger(cpu, {"value": 1})
            self.assertTrue(done.wait(5), "cpu node did not reach sink")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 2})])

    def test_cpu_node_error_is_recorded(self) -> None:
        """CpuNode process-pool errors are recorded and stop the branch."""
        calls = []
        runtime = WorkflowRuntime(max_workers=2, max_cpu_workers=1)

        bad = runtime.register(CpuNode("bad_cpu", cpu_fail))

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(bad, sink)

        runtime.start()
        try:
            runtime.trigger(bad, {"value": 1})
            for _ in range(50):
                if runtime.errors:
                    break
                threading.Event().wait(0.1)
        finally:
            runtime.stop()

        self.assertEqual(calls, [])
        self.assertEqual(len(runtime.errors), 1)
        self.assertEqual(runtime.errors[0].payload["node_id"], "bad_cpu")
        self.assertEqual(runtime.errors[0].payload["error_type"], "RuntimeError")

    def test_timer_source_emits_count_limit_events(self) -> None:
        """TimerSource emits the configured number of events."""
        payloads = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        timer = runtime.register(TimerSource("timer", interval=0.01, count_limit=3))

        def sink_fn(event: Event):
            payloads.append(dict(event.payload))
            if len(payloads) == 3:
                done.set()
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(timer, sink)

        runtime.start()
        try:
            self.assertTrue(done.wait(2), "timer did not emit expected events")
        finally:
            runtime.stop()

        self.assertEqual([payload["count"] for payload in payloads], [0, 1, 2])

    def test_timer_source_shares_async_loop(self) -> None:
        """TimerSource runs on the shared asyncio loop and still emits events."""
        payloads = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        timer = runtime.register(TimerSource("timer_shared", interval=0.01, count_limit=2))

        def sink_fn(event: Event):
            payloads.append(dict(event.payload))
            if len(payloads) == 2:
                done.set()
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(timer, sink)

        runtime.start()
        try:
            self.assertTrue(done.wait(2), "shared-loop timer did not emit expected events")
        finally:
            runtime.stop()

        self.assertEqual([payload["count"] for payload in payloads], [0, 1])

    def test_filter_node_allows_true_and_blocks_false(self) -> None:
        """FilterNode propagates True predicates and blocks False predicates."""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        source = runtime.register(FunctionNode("source", lambda event: event.payload))
        filter_node = runtime.register(
            FilterNode("filter", lambda event: event.payload["allow"])
        )

        def sink_fn(event: Event):
            calls.append(dict(event.payload))
            if len(calls) == 1:
                done.set()
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(source, filter_node)
        runtime.connect(filter_node, sink)

        runtime.start()
        try:
            runtime.trigger(source, {"allow": True})
            runtime.trigger(source, {"allow": False})
            self.assertTrue(done.wait(2), "allowed event did not reach sink")
            threading.Event().wait(0.1)
        finally:
            runtime.stop()

        self.assertEqual(calls, [{"allow": True}])

    def test_pending_count_tracks_sync_futures(self) -> None:
        """Runtime tracks unfinished sync futures and removes them when done."""
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=1)

        def slow_fn(event: Event):
            started.set()
            release.wait(2)
            done.set()
            return None

        slow = runtime.register(FunctionNode("slow", slow_fn))

        runtime.start()
        try:
            runtime.trigger(slow, {"value": 1})
            self.assertTrue(started.wait(2), "slow node did not start")
            self.assertGreaterEqual(runtime.pending_count(), 1)
            release.set()
            self.assertTrue(done.wait(2), "slow node did not finish")

            for _ in range(20):
                if runtime.pending_count() == 0:
                    break
                time.sleep(0.05)
        finally:
            runtime.stop()

        self.assertEqual(runtime.pending_count(), 0)

    def test_pending_count_tracks_async_futures(self) -> None:
        """Runtime tracks unfinished async futures and removes them when done."""
        calls = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=1)

        async def slow_async_fn(event: Event):
            await asyncio.sleep(0.1)
            calls.append(dict(event.payload))
            done.set()
            return None

        async_node = runtime.register(AsyncFunctionNode("slow_async", slow_async_fn))

        runtime.start()
        try:
            runtime.trigger(async_node, {"value": 1})
            self.assertGreaterEqual(runtime.pending_count(), 1)
            self.assertTrue(done.wait(2), "async node did not finish")

            for _ in range(20):
                if runtime.pending_count() == 0:
                    break
                time.sleep(0.05)
        finally:
            runtime.stop()

        self.assertEqual(calls, [{"value": 1}])
        self.assertEqual(runtime.pending_count(), 0)

    def test_wait_until_returns_when_event_is_set(self) -> None:
        """Runtime.wait_until blocks until the completion event is set."""
        runtime = WorkflowRuntime(max_workers=1)
        done = threading.Event()

        def signal_later() -> None:
            time.sleep(0.1)
            done.set()

        threading.Thread(target=signal_later, daemon=True).start()

        self.assertTrue(runtime.wait_until(done, timeout=1.0))

    def test_stats_reports_basic_runtime_state(self) -> None:
        """Runtime.stats returns basic runtime state without requiring psutil."""
        runtime = WorkflowRuntime(max_workers=1)

        stats = runtime.stats()

        self.assertIn("running", stats)
        self.assertIn("node_count", stats)
        self.assertIn("pending_count", stats)
        self.assertIn("error_count", stats)
        self.assertIn("timer_count", stats)
        self.assertIn("cpu_count", stats)
        self.assertIn("loop_thread_alive", stats)
        self.assertEqual(stats["pending_count"], stats["pending_stats"]["total_pending"])

    def test_pending_stats_reports_split_counts(self) -> None:
        """Runtime.pending_stats returns split counts for each executor."""
        runtime = WorkflowRuntime(max_workers=1)

        stats = runtime.pending_stats()

        self.assertIn("sync_pending", stats)
        self.assertIn("cpu_pending", stats)
        self.assertIn("async_pending", stats)
        self.assertIn("timer_pending", stats)
        self.assertIn("total_pending", stats)

    def test_hooks_observe_task_lifecycle_and_dispatch(self) -> None:
        """Runtime hooks receive task, node, and dispatch events."""
        events = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=2)

        def hook(kind: str, data: dict) -> None:
            events.append((kind, dict(data)))

        runtime.add_hook(hook)

        def start_fn(event: Event):
            return {"value": event.payload["value"] + 1}

        def sink_fn(event: Event):
            done.set()
            return None

        start = runtime.register(FunctionNode("start", start_fn))
        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(start, sink)

        runtime.start()
        try:
            runtime.trigger(start, {"value": 1})
            self.assertTrue(done.wait(2), "hook test flow did not finish")
        finally:
            runtime.stop()

        kinds = [kind for kind, _ in events]
        self.assertIn("runtime_started", kinds)
        self.assertIn("event_created", kinds)
        self.assertIn("task_submitted", kinds)
        self.assertIn("node_started", kinds)
        self.assertIn("node_finished", kinds)
        self.assertIn("task_done", kinds)
        self.assertIn("event_dispatched", kinds)

        self.assertTrue(
            any(
                kind == "event_dispatched"
                and data["source"] == "start"
                and data["target"] == "sink"
                for kind, data in events
            )
        )
        self.assertTrue(
            any(
                kind == "task_done"
                and data["node_id"] == "sink"
                and data["success"] is True
                for kind, data in events
            )
        )

    def test_hook_errors_do_not_break_runtime(self) -> None:
        """A failing hook is ignored so runtime execution can continue."""
        events = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=1)

        def broken_hook(kind: str, data: dict) -> None:
            raise RuntimeError("hook failed")

        def recorder(kind: str, data: dict) -> None:
            events.append((kind, dict(data)))
            if kind == "node_error":
                done.set()

        runtime.add_hook(broken_hook)
        runtime.add_hook(recorder)

        def bad_fn(event: Event):
            raise ValueError("bad")

        bad = runtime.register(FunctionNode("bad", bad_fn))

        runtime.start()
        try:
            runtime.trigger(bad, {"value": 1})
            self.assertTrue(done.wait(2), "node_error hook was not emitted")
        finally:
            runtime.stop()

        self.assertEqual(len(runtime.errors), 1)
        self.assertTrue(
            any(
                kind == "node_error"
                and data["node_id"] == "bad"
                and data["error_type"] == "ValueError"
                for kind, data in events
            )
        )


if __name__ == "__main__":
    unittest.main()
