from __future__ import annotations

"""Runtime for workflow nodes.

WorkflowRuntime owns registration, dispatching, thread/process executors,
the shared asyncio loop, timer scheduling, and runtime hooks.
"""

import asyncio
import os
import threading
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from typing import Any, Callable, Iterable

from .event import Event
from .graph import WorkflowGraph
from .nodes import BaseNode, ExecutionMode, FunctionNode, TimerSource


def _run_cpu_func(func, event: Event):
    """Run a CPU node function in a process pool worker."""
    return func(event)


class WorkflowRuntime:
    """Unified runtime for a node graph."""

    def __init__(self, max_workers: int = 8, max_cpu_workers: int | None = None):
        """Create executors, graph, and runtime bookkeeping."""
        self.max_workers = max_workers
        self.max_cpu_workers = max_cpu_workers
        self.io_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.cpu_executor = ProcessPoolExecutor(max_workers=max_cpu_workers)
        self.loop = asyncio.new_event_loop()
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
        self._loop_thread: threading.Thread | None = None

    def register(self, node: BaseNode) -> BaseNode:
        """Register a node and bind the runtime to it."""
        self.graph.add_node(node)
        node.bind_runtime(self)
        return node

    def connect(self, source: BaseNode, target: BaseNode) -> BaseNode:
        """Connect two nodes in the graph."""
        return self.graph.connect(source, target)

    def add_hook(
        self, hook: Callable[[str, dict[str, Any]], None]
    ) -> Callable[[str, dict[str, Any]], None]:
        """Register a runtime hook."""
        with self._hook_lock:
            if hook not in self._hooks:
                self._hooks.append(hook)
        return hook

    def remove_hook(self, hook: Callable[[str, dict[str, Any]], None]) -> None:
        """Remove a previously registered hook."""
        with self._hook_lock:
            if hook in self._hooks:
                self._hooks.remove(hook)

    def _emit_hook(self, kind: str, **data: Any) -> None:
        """Notify all hooks about a runtime event."""
        with self._hook_lock:
            hooks = list(self._hooks)
        for hook in hooks:
            try:
                hook(kind, data)
            except Exception:
                continue

    def start(self) -> None:
        """Start the runtime."""
        if self.running:
            return
        self.running = True
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()
        for node in self.nodes.values():
            node.start()
            if isinstance(node, TimerSource):
                self._start_timer(node)
        self._emit_hook("runtime_started", node_count=len(self.nodes))

    def stop(self) -> None:
        """Stop the runtime and wait for submitted work to settle."""
        if not self.running:
            return
        self._emit_hook("runtime_stopping")
        self.running = False
        for node in self.nodes.values():
            node.stop()
        for future in self._timer_futures:
            future.cancel()
        for future in self._timer_futures:
            try:
                future.result(timeout=1.0)
            except Exception:
                pass
        self._timer_futures.clear()
        drain = asyncio.run_coroutine_threadsafe(asyncio.sleep(0), self.loop)
        try:
            drain.result(timeout=1.0)
        except Exception:
            pass
        self.loop.call_soon_threadsafe(self.loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=1.0)
            self._loop_thread = None
        if not self.loop.is_closed():
            self.loop.close()
        self.io_executor.shutdown(wait=True, cancel_futures=False)
        self.cpu_executor.shutdown(wait=True, cancel_futures=False)
        self._emit_hook("runtime_stopped")

    def _run_loop(self) -> None:
        """Run the shared asyncio loop in a dedicated thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _start_timer(self, node: TimerSource) -> None:
        """Start one timer task on the shared asyncio loop."""
        future = asyncio.run_coroutine_threadsafe(self._run_timer(node), self.loop)
        self._timer_futures.append(future)
        self._emit_hook(
            "task_submitted",
            node_id=node.node_id,
            event_id=None,
            executor="timer",
        )

    async def _run_timer(self, node: TimerSource) -> None:
        """Run one timer task on the shared asyncio loop."""
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
        """Create an event from source and dispatch it to downstream nodes."""
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
        """Trigger a node directly."""
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
        """Forward an existing event."""
        event.source = source.node_id
        self.dispatch(source, event)

    def _build_event(self, event: Event, source: BaseNode, target: BaseNode) -> Event:
        """Clone an event for the next downstream hop."""
        target_event = event.fork(target.node_id)
        target_event.source = source.node_id
        target_event.route_targets = None
        return target_event

    def dispatch(self, source: BaseNode, event: Event) -> None:
        """Dispatch an event to downstream nodes."""
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
        """Broadcast an event to a fixed list of targets."""
        for target in targets:
            target_event = event.fork(target.node_id)
            self._submit(target, target_event)

    def pending_count(self) -> int:
        """Return the total number of unfinished tasks."""
        return self.pending_stats()["total_pending"]

    def pending_stats(self) -> dict[str, int]:
        """Return a split view of unfinished tasks by executor."""
        with self._future_lock:
            return self._pending_stats_unlocked()

    def stats(self) -> dict:
        """Return runtime state and optional process resource data."""
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
        """Build the pending snapshot while holding _future_lock."""
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
        """Block until an external completion signal is set."""
        return done_event.wait(timeout)

    def _track_future(self, future: Future, bucket: set[Future]) -> Future:
        """Track a future and remove it automatically when done."""
        with self._future_lock:
            bucket.add(future)

        def cleanup(done: Future) -> None:
            with self._future_lock:
                bucket.discard(done)

        future.add_done_callback(cleanup)
        return future

    def _submit(self, target: BaseNode, event: Event) -> None:
        """Submit a task to the correct executor."""
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
            future = self.cpu_executor.submit(_run_cpu_func, target.func, event)
            self._track_future(future, self._cpu_futures)
            future.add_done_callback(
                lambda result: self._handle_cpu_result(target, event, result)
            )
            return
        if target.execution_mode == ExecutionMode.ASYNC:
            future = asyncio.run_coroutine_threadsafe(
                self._execute_node_async(target, event), self.loop
            )
            self._track_future(future, self._async_futures)
            return
        future = self.io_executor.submit(self._execute_node_sync, target, event)
        self._track_future(future, self._sync_futures)

    def _execute_node_sync(self, node: BaseNode, event: Event) -> None:
        """Execute a sync node in the thread pool."""
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
        """Handle the final result of a process-pool task."""
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
        """Execute an async node on the shared asyncio loop."""
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
        """Record a node error and stop propagation on this branch."""
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
        """Convert a node result into downstream dispatch behavior."""
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
