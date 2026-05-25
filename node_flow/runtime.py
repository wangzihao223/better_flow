from __future__ import annotations

"""运行时调度器。

WorkflowRuntime 统一管理节点注册、事件分发、线程池、进程池、asyncio
事件循环和定时触发，不让节点自己掌控执行资源。
"""

import asyncio
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from typing import Dict, Iterable

from .event import Event
from .nodes import BaseNode, ExecutionMode, TimerSource


class WorkflowRuntime:
    """节点图的统一运行时。"""

    def __init__(self, max_workers: int = 8, max_cpu_workers: int | None = None):
        self.max_workers = max_workers
        self.max_cpu_workers = max_cpu_workers
        self.io_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.cpu_executor = ProcessPoolExecutor(max_workers=max_cpu_workers)
        self.loop = asyncio.new_event_loop()
        self.nodes: Dict[str, BaseNode] = {}
        self.running = False
        self._loop_thread: threading.Thread | None = None
        self._timer_threads: list[threading.Thread] = []

    def register(self, node: BaseNode) -> BaseNode:
        self.nodes[node.node_id] = node
        node.bind_runtime(self)
        return node

    def connect(self, source: BaseNode, target: BaseNode) -> BaseNode:
        source.connect(target)
        return target

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._loop_thread = threading.Thread(target=self._run_loop, daemon=True)
        self._loop_thread.start()
        for node in self.nodes.values():
            node.start()
            if isinstance(node, TimerSource):
                self._start_timer(node)

    def stop(self) -> None:
        if not self.running:
            return
        self.running = False
        for node in self.nodes.values():
            node.stop()
        for thread in self._timer_threads:
            thread.join(timeout=1.0)
        self._timer_threads.clear()
        self.loop.call_soon_threadsafe(self.loop.stop)
        if self._loop_thread is not None:
            self._loop_thread.join(timeout=1.0)
            self._loop_thread = None
        self.io_executor.shutdown(wait=True, cancel_futures=False)
        self.cpu_executor.shutdown(wait=True, cancel_futures=False)

    def _run_loop(self) -> None:
        # 单独起一个线程承载 asyncio loop，供异步节点调度。
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _start_timer(self, node: TimerSource) -> None:
        # TimerSource 由 runtime 统一轮询，不允许节点自己开循环。
        def loop() -> None:
            count = 0
            while self.running and (node.count_limit is None or count < node.count_limit):
                payload = node.make_payload(count)
                self.emit(node, payload, name="tick")
                count += 1
                time.sleep(node.interval)

        thread = threading.Thread(target=loop, daemon=True)
        self._timer_threads.append(thread)
        thread.start()

    def emit(self, source: BaseNode, payload, name: str = "event") -> Event:
        event = Event(source=source.node_id, name=name, payload=payload)
        self.dispatch(source, event)
        return event

    def forward(self, source: BaseNode, event: Event) -> None:
        event.source = source.node_id
        self.dispatch(source, event)

    def _build_event(self, event: Event, source: BaseNode, target: BaseNode) -> Event:
        target_event = event.fork(target.node_id)
        target_event.source = source.node_id
        return target_event

    def dispatch(self, source: BaseNode, event: Event) -> None:
        for target in list(source.output_nodes):
            target_event = self._build_event(event, source, target)
            self._submit(target, target_event)

    def broadcast(self, targets: Iterable[BaseNode], event: Event) -> None:
        for target in targets:
            target_event = event.fork(target.node_id)
            self._submit(target, target_event)

    def _submit(self, target: BaseNode, event: Event) -> None:
        if not self.running:
            return
        if target.execution_mode == ExecutionMode.CPU:
            future = self.cpu_executor.submit(target.process, event)
            future.add_done_callback(lambda result: self._handle_cpu_result(target, event, result))
            return
        if target.execution_mode == ExecutionMode.ASYNC:
            asyncio.run_coroutine_threadsafe(self._execute_node_async(target, event), self.loop)
            return
        self.io_executor.submit(self._execute_node_sync, target, event)

    def _execute_node_sync(self, node: BaseNode, event: Event) -> None:
        if not self.running or not node.running:
            return
        result = node.process(event)
        self._handle_result(node, event, result)

    def _handle_cpu_result(self, node: BaseNode, event: Event, future) -> None:
        if not self.running or not node.running:
            return
        self._handle_result(node, event, future.result())

    async def _execute_node_async(self, node: BaseNode, event: Event) -> None:
        if not self.running or not node.running:
            return
        result = await node.process_async(event)  # type: ignore[attr-defined]
        self._handle_result(node, event, result)

    def _handle_result(self, node: BaseNode, event: Event, result) -> None:
        # 节点返回 Event / list / 普通值时，统一转成下游可消费的事件。
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
