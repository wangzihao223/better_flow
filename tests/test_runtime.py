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
    raise RuntimeError("CPU 执行失败")


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
            self.assertTrue(done.wait(2), "trigger 没有到达下游节点")
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
            self.assertTrue(done.wait(2), "emit 没有到达下游节点")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 1})])

    def test_router_dispatches_only_selected_branch_once(self) -> None:
        """RouterNode 只过滤当前一跳，并允许选中的分支继续传播。"""
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
            self.assertTrue(done.wait(2), "选中的分支没有到达 sink")
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
        """RouterNode 返回空列表表示不选择任何下游分支。"""
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
        """同步节点失败时会记录错误事件，并且不调用下游节点。"""
        calls = []
        runtime = WorkflowRuntime(max_workers=2)

        def bad_fn(event: Event):
            raise ValueError("无效 payload")

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
        self.assertEqual(error.payload["error_message"], "无效 payload")
        self.assertEqual(error.payload["payload"], {"value": 1})

    def test_async_node_result_reaches_downstream(self) -> None:
        """AsyncFunctionNode 会在 asyncio loop 上执行，并传播处理结果。"""
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
            self.assertTrue(done.wait(2), "异步节点没有到达 sink")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 2})])

    def test_async_node_error_is_recorded(self) -> None:
        """AsyncFunctionNode 异常会被记录，并停止当前分支。"""
        calls = []
        runtime = WorkflowRuntime(max_workers=2)

        async def bad_async_fn(event: Event):
            raise RuntimeError("异步执行失败")

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
        """CpuNode 会在进程池中执行，并传播处理结果。"""
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
            self.assertTrue(done.wait(5), "CPU 节点没有到达 sink")
        finally:
            runtime.stop()

        self.assertEqual(calls, [("sink", {"value": 2})])

    def test_cpu_node_error_is_recorded(self) -> None:
        """CpuNode 进程池异常会被记录，并停止当前分支。"""
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

    def test_cpu_submit_error_is_recorded(self) -> None:
        """CPU 执行器提交失败时应记录错误，而不是从 trigger 抛出。"""
        calls = []
        runtime = WorkflowRuntime(max_workers=2, max_cpu_workers=1)

        class BrokenCpuExecutor:
            def submit(self, *args, **kwargs):
                raise PermissionError("CPU 执行器不可用")

            def shutdown(self, *args, **kwargs):
                return None

        cpu = runtime.register(CpuNode("cpu", cpu_add_one))

        def sink_fn(event: Event):
            calls.append(("sink", dict(event.payload)))
            return None

        sink = runtime.register(FunctionNode("sink", sink_fn))
        runtime.connect(cpu, sink)

        runtime.start()
        try:
            runtime.cpu_executor = BrokenCpuExecutor()  # type: ignore[assignment]
            runtime.trigger(cpu, {"value": 1})
        finally:
            runtime.stop()

        self.assertEqual(calls, [])
        self.assertEqual(len(runtime.errors), 1)
        self.assertEqual(runtime.errors[0].payload["node_id"], "cpu")
        self.assertEqual(runtime.errors[0].payload["error_type"], "PermissionError")

    def test_timer_source_emits_count_limit_events(self) -> None:
        """TimerSource 会按 count_limit 发出指定数量的事件。"""
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
            self.assertTrue(done.wait(2), "timer 没有发出预期事件")
        finally:
            runtime.stop()

        self.assertEqual([payload["count"] for payload in payloads], [0, 1, 2])

    def test_timer_source_shares_async_loop(self) -> None:
        """TimerSource 在共享 asyncio loop 上运行，并能正常发出事件。"""
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
            self.assertTrue(done.wait(2), "共享 loop 的 timer 没有发出预期事件")
        finally:
            runtime.stop()

        self.assertEqual([payload["count"] for payload in payloads], [0, 1])

    def test_filter_node_allows_true_and_blocks_false(self) -> None:
        """FilterNode 放行 True 条件，并阻断 False 条件。"""
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
            self.assertTrue(done.wait(2), "放行事件没有到达 sink")
            threading.Event().wait(0.1)
        finally:
            runtime.stop()

        self.assertEqual(calls, [{"allow": True}])

    def test_pending_count_tracks_sync_futures(self) -> None:
        """Runtime 会追踪未完成的同步 future，并在完成后移除。"""
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
            self.assertTrue(started.wait(2), "慢节点没有启动")
            self.assertGreaterEqual(runtime.pending_count(), 1)
            release.set()
            self.assertTrue(done.wait(2), "慢节点没有完成")

            for _ in range(20):
                if runtime.pending_count() == 0:
                    break
                time.sleep(0.05)
        finally:
            runtime.stop()

        self.assertEqual(runtime.pending_count(), 0)

    def test_pending_count_tracks_async_futures(self) -> None:
        """Runtime 会追踪未完成的异步 future，并在完成后移除。"""
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
            self.assertTrue(done.wait(2), "异步节点没有完成")

            for _ in range(20):
                if runtime.pending_count() == 0:
                    break
                time.sleep(0.05)
        finally:
            runtime.stop()

        self.assertEqual(calls, [{"value": 1}])
        self.assertEqual(runtime.pending_count(), 0)

    def test_wait_until_returns_when_event_is_set(self) -> None:
        """Runtime.wait_until 会阻塞到完成事件被设置。"""
        runtime = WorkflowRuntime(max_workers=1)
        done = threading.Event()

        def signal_later() -> None:
            time.sleep(0.1)
            done.set()

        threading.Thread(target=signal_later, daemon=True).start()

        self.assertTrue(runtime.wait_until(done, timeout=1.0))

    def test_start_waits_until_async_loop_is_ready(self) -> None:
        """start 会等待共享 asyncio loop 可以执行任务后再返回。"""
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=1)

        async def mark_done(event: Event):
            done.set()
            return None

        async_node = runtime.register(AsyncFunctionNode("async_ready", mark_done))

        runtime.start()
        try:
            runtime.trigger(async_node, {"value": 1})
            self.assertTrue(done.wait(2), "start 返回后 async loop 仍未 ready")
        finally:
            runtime.stop()

    def test_stop_closes_loop_and_prevents_restart(self) -> None:
        """stop 会关闭 runtime 资源，并明确禁止 restart。"""
        runtime = WorkflowRuntime(max_workers=1)

        runtime.start()
        runtime.stop()

        self.assertFalse(runtime.running)
        self.assertIsNotNone(runtime.loop)
        self.assertTrue(runtime.loop.is_closed())
        self.assertIsNone(runtime._loop_thread)

        with self.assertRaises(RuntimeError):
            runtime.start()

    def test_stats_reports_basic_runtime_state(self) -> None:
        """Runtime.stats 不依赖 psutil 也会返回基础运行状态。"""
        runtime = WorkflowRuntime(max_workers=1)

        stats = runtime.stats()

        self.assertIn("running", stats)
        self.assertIn("node_count", stats)
        self.assertIn("pending_count", stats)
        self.assertIn("error_count", stats)
        self.assertIn("timer_count", stats)
        self.assertIn("cpu_count", stats)
        self.assertIn("loop_thread_alive", stats)
        self.assertIn("stopping", stats)
        self.assertEqual(stats["pending_count"], stats["pending_stats"]["total_pending"])

    def test_pending_stats_reports_split_counts(self) -> None:
        """Runtime.pending_stats 会按执行器返回 pending 数量。"""
        runtime = WorkflowRuntime(max_workers=1)

        stats = runtime.pending_stats()

        self.assertIn("sync_pending", stats)
        self.assertIn("cpu_pending", stats)
        self.assertIn("async_pending", stats)
        self.assertIn("timer_pending", stats)
        self.assertIn("total_pending", stats)

    def test_hooks_observe_task_lifecycle_and_dispatch(self) -> None:
        """Runtime hook 能观察任务、节点和事件分发过程。"""
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
            self.assertTrue(done.wait(2), "hook 测试流程没有完成")
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
        """hook 自身异常会被忽略，不会打断 runtime 执行。"""
        events = []
        done = threading.Event()
        runtime = WorkflowRuntime(max_workers=1)

        def broken_hook(kind: str, data: dict) -> None:
            raise RuntimeError("hook 执行失败")

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
            self.assertTrue(done.wait(2), "没有发出 node_error hook")
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
