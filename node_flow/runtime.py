from __future__ import annotations

"""节点工作流运行时。

WorkflowRuntime 负责节点注册、事件分发、线程池/进程池、共享 asyncio
事件循环、定时器调度和运行时 hook。
"""

import asyncio
import multiprocessing
import os
import threading
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from typing import Any, Callable, Iterable

from .event import Event
from .graph import WorkflowGraph
from .nodes import BaseNode, ExecutionMode, FunctionNode, TimerSource


def _run_cpu_func(func, event: Event):
    """在进程池 worker 中执行 CPU 节点函数。"""
    return func(event)


def _warm_cpu_pool() -> None:
    """用于在 runtime 子线程启动前预热进程池 worker 的空函数。"""
    return None


class WorkflowRuntime:
    """节点图的统一运行时。"""

    def __init__(self, max_workers: int = 8, max_cpu_workers: int | None = None):
        """创建执行器、图结构和运行时状态。"""
        self.max_workers = max_workers
        self.max_cpu_workers = max_cpu_workers
        self.io_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.cpu_executor: ProcessPoolExecutor | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.graph = WorkflowGraph()
        self.nodes = self.graph.nodes
        self.errors: list[Event] = []
        self._sync_futures: set[Future] = set()
        self._cpu_futures: set[Future] = set()
        self._async_futures: set[Future] = set()
        self._future_lock = threading.Lock()
        self._timer_futures: list[Future] = []
        self._hooks: list[Callable[[str, dict[str, Any]], None]] = []
        self._hook_lock = threading.Lock()
        self.running = False
        self.stopping = False
        self._started_once = False
        self._loop_ready = threading.Event()
        self._loop_stopped = threading.Event()
        self._loop_thread: threading.Thread | None = None

    def register(self, node: BaseNode) -> BaseNode:
        """注册节点，并把当前 runtime 绑定到节点上。"""
        self.graph.add_node(node)
        node.bind_runtime(self)
        return node

    def connect(self, source: BaseNode, target: BaseNode) -> BaseNode:
        """连接图中的两个节点。"""
        return self.graph.connect(source, target)

    def add_hook(
        self, hook: Callable[[str, dict[str, Any]], None]
    ) -> Callable[[str, dict[str, Any]], None]:
        """注册运行时 hook。"""
        with self._hook_lock:
            if hook not in self._hooks:
                self._hooks.append(hook)
        return hook

    def remove_hook(self, hook: Callable[[str, dict[str, Any]], None]) -> None:
        """移除已注册的运行时 hook。"""
        with self._hook_lock:
            if hook in self._hooks:
                self._hooks.remove(hook)

    def _emit_hook(self, kind: str, **data: Any) -> None:
        """把运行时事件通知给所有 hook。"""
        with self._hook_lock:
            hooks = list(self._hooks)
        for hook in hooks:
            try:
                hook(kind, data)
            except Exception:
                continue

    def start(self) -> None:
        """启动 runtime。"""
        if self.running:
            return
        if self._started_once:
            raise RuntimeError("WorkflowRuntime cannot be restarted after stop()")
        self._started_once = True
        self._loop_ready.clear()
        self._loop_stopped.clear()
        self.stopping = False
        self._warm_cpu_executor_if_needed()
        self.running = True
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()
        if not self._loop_ready.wait(timeout=5.0):
            self.running = False
            self.stopping = False
            raise RuntimeError("asyncio loop 未能启动")
        for node in self.nodes.values():
            node.start()
            if isinstance(node, TimerSource):
                self._start_timer(node)
        self._emit_hook("runtime_started", node_count=len(self.nodes))

    def stop(self) -> None:
        """停止 runtime，并等待已提交的运行时资源完成收尾。"""
        if not self.running:
            return
        self._emit_hook("runtime_stopping")
        self.stopping = True
        self.running = False
        for node in self.nodes.values():
            node.stop()
        loop = self.loop
        if loop is not None and not loop.is_closed():
            shutdown = asyncio.run_coroutine_threadsafe(self._shutdown_loop(), loop)
            try:
                shutdown.result(timeout=5.0)
            except Exception:
                pass
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=5.0)
            if self._loop_thread.is_alive():
                raise RuntimeError("asyncio loop 子线程未能停止")
            self._loop_thread = None
        self.io_executor.shutdown(wait=True, cancel_futures=False)
        if self.cpu_executor is not None:
            self.cpu_executor.shutdown(wait=True, cancel_futures=False)
        self.stopping = False
        self._emit_hook("runtime_stopped")

    def _run_loop(self) -> None:
        """在专用子线程中运行共享 asyncio loop。"""
        loop = asyncio.new_event_loop()
        self.loop = loop
        asyncio.set_event_loop(loop)
        try:
            self._loop_ready.set()
            loop.run_forever()
        finally:
            if not loop.is_closed():
                loop.close()
            self._loop_stopped.set()

    async def _shutdown_loop(self) -> None:
        """取消 loop 持有的任务，并停止事件循环。"""
        current = asyncio.current_task()
        for future in self._timer_futures:
            future.cancel()
        self._timer_futures.clear()
        await self._cancel_loop_tasks(exclude={current} if current is not None else None)
        self.stopping = False
        if self.loop is not None:
            self.loop.stop()

    async def _cancel_loop_tasks(
        self, exclude: set[asyncio.Task] | None = None
    ) -> None:
        """在 loop 停止前取消未完成的 asyncio task。"""
        if self.loop is None:
            return
        current = asyncio.current_task()
        excluded = set(exclude or ())
        if current is not None:
            excluded.add(current)
        tasks = [
            task
            for task in asyncio.all_tasks(self.loop)
            if task not in excluded and not task.done()
        ]
        if not tasks:
            return
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _start_timer(self, node: TimerSource) -> None:
        """在共享 asyncio loop 上启动一个 timer task。"""
        if self.loop is None:
            raise RuntimeError("asyncio loop is not initialized")
        future = asyncio.run_coroutine_threadsafe(self._run_timer(node), self.loop)
        self._timer_futures.append(future)
        self._emit_hook(
            "task_submitted",
            node_id=node.node_id,
            event_id=None,
            executor="timer",
        )

    async def _run_timer(self, node: TimerSource) -> None:
        """在共享 asyncio loop 上运行一个 timer task。"""
        try:
            count = 0
            while self.running and (
                node.count_limit is None or count < node.count_limit
            ):
                payload = node.make_payload(count)
                self._emit_hook(
                    "timer_tick",
                    node_id=node.node_id,
                    count=count,
                    interval=node.interval,
                )
                self.emit(node, payload, name="tick")
                count += 1
                await asyncio.sleep(node.interval)
        except asyncio.CancelledError:
            return

    def emit(self, source: BaseNode, payload, name: str = "event") -> Event:
        """从 source 创建事件，并分发给下游节点。"""
        event = Event(source=source.node_id, name=name, payload=payload)
        self._emit_hook(
            "event_created",
            source=source.node_id,
            event_id=event.event_id,
            name=event.name,
        )
        self.dispatch(source, event)
        return event

    def trigger(self, node: BaseNode, payload, name: str = "event") -> Event:
        """直接触发某个节点。"""
        event = Event(source="runtime", name=name, payload=payload)
        self._emit_hook(
            "event_created",
            source="runtime",
            event_id=event.event_id,
            name=event.name,
        )
        self._submit(node, event)
        return event

    def forward(self, source: BaseNode, event: Event) -> None:
        """转发一个已有事件。"""
        event.source = source.node_id
        self.dispatch(source, event)

    def _build_event(self, event: Event, source: BaseNode, target: BaseNode) -> Event:
        """为下一跳下游节点复制事件。"""
        target_event = event.fork(target.node_id)
        target_event.source = source.node_id
        target_event.route_targets = None
        return target_event

    def dispatch(self, source: BaseNode, event: Event) -> None:
        """把事件分发给 source 的下游节点。"""
        targets = self.graph.downstream(source)
        if event.route_targets is not None:
            route_targets = set(event.route_targets)
            targets = [target for target in targets if target.node_id in route_targets]
        for target in targets:
            target_event = self._build_event(event, source, target)
            self._emit_hook(
                "event_dispatched",
                source=source.node_id,
                target=target.node_id,
                event_id=target_event.event_id,
                name=target_event.name,
                route_targets=event.route_targets,
            )
            self._submit(target, target_event)

    def broadcast(self, targets: Iterable[BaseNode], event: Event) -> None:
        """把事件广播给指定目标节点列表。"""
        for target in targets:
            target_event = event.fork(target.node_id)
            self._submit(target, target_event)

    def pending_count(self) -> int:
        """返回未完成业务任务总数。"""
        return self.pending_stats()["total_pending"]

    def pending_stats(self) -> dict[str, int]:
        """按执行器类型返回未完成任务数量。"""
        with self._future_lock:
            return self._pending_stats_unlocked()

    def stats(self) -> dict:
        """返回 runtime 状态和可选的进程资源信息。"""
        pending_stats = self.pending_stats()
        data = {
            "running": self.running,
            "node_count": len(self.nodes),
            "pending_count": pending_stats["total_pending"],
            "pending_stats": pending_stats,
            "error_count": len(self.errors),
            "timer_count": len(self._timer_futures),
            "cpu_count": os.cpu_count(),
            "loop_thread_alive": (
                self._loop_thread is not None and self._loop_thread.is_alive()
            ),
            "stopping": self.stopping,
        }

        try:
            import psutil  # type: ignore[import-not-found]
        except ImportError:
            data["psutil_available"] = False
            return data

        process = psutil.Process()
        data.update(
            {
                "psutil_available": True,
                "pid": process.pid,
                "cpu_percent": process.cpu_percent(interval=None),
                "memory_rss": process.memory_info().rss,
                "thread_count": process.num_threads(),
            }
        )

        try:
            data["cpu_num"] = process.cpu_num()
        except Exception:
            data["cpu_num"] = None

        try:
            data["cpu_affinity"] = process.cpu_affinity()
        except Exception:
            data["cpu_affinity"] = None

        return data

    def _pending_stats_unlocked(self) -> dict[str, int]:
        """在持有 _future_lock 时构建 pending 快照。"""
        sync_pending = len(self._sync_futures)
        cpu_pending = len(self._cpu_futures)
        async_pending = len(self._async_futures)
        timer_pending = len(self._timer_futures)
        return {
            "sync_pending": sync_pending,
            "cpu_pending": cpu_pending,
            "async_pending": async_pending,
            "timer_pending": timer_pending,
            "total_pending": sync_pending + cpu_pending + async_pending,
        }

    def wait_until(
        self, done_event: threading.Event, timeout: float | None = None
    ) -> bool:
        """阻塞等待外部完成信号。"""
        return done_event.wait(timeout)

    def _track_future(self, future: Future, bucket: set[Future]) -> Future:
        """追踪 future，并在完成后自动从集合中移除。"""
        with self._future_lock:
            bucket.add(future)

        def cleanup(done: Future) -> None:
            with self._future_lock:
                bucket.discard(done)

        future.add_done_callback(cleanup)
        return future

    def _ensure_cpu_executor(self) -> ProcessPoolExecutor:
        """仅在提交 CPU 节点时创建 CPU 进程池。"""
        if self.cpu_executor is not None:
            return self.cpu_executor
        mp_context = None
        if "fork" in multiprocessing.get_all_start_methods():
            mp_context = multiprocessing.get_context("fork")
        self.cpu_executor = ProcessPoolExecutor(
            max_workers=self.max_cpu_workers,
            mp_context=mp_context,
        )
        return self.cpu_executor

    def _warm_cpu_executor_if_needed(self) -> None:
        """如果图中已有 CPU 节点，在 asyncio loop 子线程创建前预热进程池。"""
        if not any(node.execution_mode == ExecutionMode.CPU for node in self.nodes.values()):
            return
        try:
            future = self._ensure_cpu_executor().submit(_warm_cpu_pool)
            future.result(timeout=5.0)
        except Exception:
            if self.cpu_executor is not None:
                self.cpu_executor.shutdown(wait=False, cancel_futures=True)
                self.cpu_executor = None

    def _submit(self, target: BaseNode, event: Event) -> None:
        """把任务提交到匹配的执行器。"""
        if not self.running:
            return
        self._emit_hook(
            "task_submitted",
            node_id=target.node_id,
            event_id=event.event_id,
            executor=target.execution_mode.value,
        )
        if target.execution_mode == ExecutionMode.CPU:
            if not isinstance(target, FunctionNode):
                self._handle_error(
                    target,
                    event,
                    TypeError("CPU nodes must expose a picklable func attribute"),
                )
                self._emit_hook(
                    "task_done",
                    node_id=target.node_id,
                    event_id=event.event_id,
                    executor=target.execution_mode.value,
                    success=False,
                )
                return
            self._emit_hook(
                "node_started",
                node_id=target.node_id,
                event_id=event.event_id,
                executor=target.execution_mode.value,
            )
            try:
                cpu_executor = self._ensure_cpu_executor()
                future = cpu_executor.submit(_run_cpu_func, target.func, event)
            except Exception as exc:
                self._handle_error(target, event, exc)
                self._emit_hook(
                    "task_done",
                    node_id=target.node_id,
                    event_id=event.event_id,
                    executor=target.execution_mode.value,
                    success=False,
                )
                return
            self._track_future(future, self._cpu_futures)
            future.add_done_callback(
                lambda result: self._handle_cpu_result(target, event, result)
            )
            return
        if target.execution_mode == ExecutionMode.ASYNC:
            if self.loop is None:
                self._handle_error(
                    target, event, RuntimeError("asyncio loop is not initialized")
                )
                self._emit_hook(
                    "task_done",
                    node_id=target.node_id,
                    event_id=event.event_id,
                    executor=target.execution_mode.value,
                    success=False,
                )
                return
            future = asyncio.run_coroutine_threadsafe(
                self._execute_node_async(target, event), self.loop
            )
            self._track_future(future, self._async_futures)
            return
        future = self.io_executor.submit(self._execute_node_sync, target, event)
        self._track_future(future, self._sync_futures)

    def _execute_node_sync(self, node: BaseNode, event: Event) -> None:
        """在线程池中执行同步节点。"""
        if not self.running or not node.running:
            return
        self._emit_hook(
            "node_started",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
        )
        try:
            result = node.process(event)
        except Exception as exc:
            self._handle_error(node, event, exc)
            self._emit_hook(
                "task_done",
                node_id=node.node_id,
                event_id=event.event_id,
                executor=node.execution_mode.value,
                success=False,
            )
            return
        self._handle_result(node, event, result)
        self._emit_hook(
            "node_finished",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
        )
        self._emit_hook(
            "task_done",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
            success=True,
        )

    def _handle_cpu_result(self, node: BaseNode, event: Event, future) -> None:
        """处理进程池任务的最终结果。"""
        if not self.running or not node.running:
            return
        try:
            result = future.result()
        except Exception as exc:
            self._handle_error(node, event, exc)
            self._emit_hook(
                "task_done",
                node_id=node.node_id,
                event_id=event.event_id,
                executor=node.execution_mode.value,
                success=False,
            )
            return
        self._handle_result(node, event, result)
        self._emit_hook(
            "node_finished",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
        )
        self._emit_hook(
            "task_done",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
            success=True,
        )

    async def _execute_node_async(self, node: BaseNode, event: Event) -> None:
        """在共享 asyncio loop 上执行异步节点。"""
        if not self.running or not node.running:
            return
        self._emit_hook(
            "node_started",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
        )
        try:
            result = await node.process_async(event)  # type: ignore[attr-defined]
        except Exception as exc:
            self._handle_error(node, event, exc)
            self._emit_hook(
                "task_done",
                node_id=node.node_id,
                event_id=event.event_id,
                executor=node.execution_mode.value,
                success=False,
            )
            return
        self._handle_result(node, event, result)
        self._emit_hook(
            "node_finished",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
        )
        self._emit_hook(
            "task_done",
            node_id=node.node_id,
            event_id=event.event_id,
            executor=node.execution_mode.value,
            success=True,
        )

    def _handle_error(self, node: BaseNode, event: Event, exc: Exception) -> None:
        """记录节点错误，并停止当前分支继续传播。"""
        error_event = Event(
            source=node.node_id,
            name="error",
            payload={
                "node_id": node.node_id,
                "event_id": event.event_id,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "payload": event.payload,
            },
            trace=[*event.trace],
        )
        self.errors.append(error_event)
        self._emit_hook(
            "node_error",
            node_id=node.node_id,
            event_id=event.event_id,
            error_type=type(exc).__name__,
            error_message=str(exc),
        )

    def _handle_result(self, node: BaseNode, event: Event, result) -> None:
        """把节点返回值转换成下游分发行为。"""
        if result is None:
            return
        if isinstance(result, Event):
            self.dispatch(node, result)
            return
        if isinstance(result, list):
            for item in result:
                if isinstance(item, Event):
                    self.dispatch(node, item)
                else:
                    self.dispatch(node, replace(event, payload=item))
            return
        self.dispatch(node, replace(event, payload=result))
