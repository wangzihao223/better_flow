from __future__ import annotations

"""运行时调度器。

WorkflowRuntime 负责执行层：节点注册、事件分发、线程池、进程池、asyncio
事件循环和定时触发。图结构本身交给 WorkflowGraph 维护。
"""

import asyncio
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from typing import Iterable

from .event import Event
from .graph import WorkflowGraph
from .nodes import BaseNode, ExecutionMode, TimerSource


class WorkflowRuntime:
    """节点图的统一运行时。"""

    def __init__(self, max_workers: int = 8, max_cpu_workers: int | None = None):
        """初始化运行时资源。

        max_workers 控制同步/IO 线程池大小，max_cpu_workers 控制 CPU 进程池大小。
        """
        self.max_workers = max_workers
        self.max_cpu_workers = max_cpu_workers
        self.io_executor = ThreadPoolExecutor(max_workers=max_workers)
        self.cpu_executor = ProcessPoolExecutor(max_workers=max_cpu_workers)
        self.loop = asyncio.new_event_loop()
        self.graph = WorkflowGraph()
        self.nodes = self.graph.nodes
        self.running = False
        self._loop_thread: threading.Thread | None = None
        self._timer_threads: list[threading.Thread] = []

    def register(self, node: BaseNode) -> BaseNode:
        """注册节点，并把 runtime 绑定到节点上。"""
        self.graph.add_node(node)
        node.bind_runtime(self)
        return node

    def connect(self, source: BaseNode, target: BaseNode) -> BaseNode:
        """连接两个节点，表示 source 的事件可以流向 target。"""
        return self.graph.connect(source, target)

    def start(self) -> None:
        """启动运行时。

        这里只打开 runtime/node 状态和异步事件循环；普通节点不会立刻执行，
        只有事件到达时才会被调度。
        """
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
        """停止运行时，并等待已经提交的任务完成。"""
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
        """在独立线程中运行 asyncio 事件循环。"""
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _start_timer(self, node: TimerSource) -> None:
        """为 TimerSource 启动由 runtime 管理的定时触发循环。"""

        def loop() -> None:
            count = 0
            while self.running and (
                node.count_limit is None or count < node.count_limit
            ):
                payload = node.make_payload(count)
                self.emit(node, payload, name="tick")
                count += 1
                time.sleep(node.interval)

        thread = threading.Thread(target=loop, daemon=True)
        self._timer_threads.append(thread)
        thread.start()

    def emit(self, source: BaseNode, payload, name: str = "event") -> Event:
        """从 source 节点创建一个新事件，并分发给它的下游节点。"""
        event = Event(source=source.node_id, name=name, payload=payload)
        self.dispatch(source, event)
        return event

    def trigger(self, node: BaseNode, payload, name: str = "event") -> Event:
        """把事件投递给指定节点自己，触发该节点执行。"""
        event = Event(source="runtime", name=name, payload=payload)
        self._submit(node, event)
        return event

    def forward(self, source: BaseNode, event: Event) -> None:
        """转发已有事件。

        目前主要保留给兼容旧式节点调用；新节点更推荐通过返回值继续传播。
        """
        event.source = source.node_id
        self.dispatch(source, event)

    def _build_event(self, event: Event, source: BaseNode, target: BaseNode) -> Event:
        """复制事件并记录它即将流向的目标节点。"""
        target_event = event.fork(target.node_id)
        target_event.source = source.node_id
        target_event.route_targets = []
        return target_event

    def dispatch(self, source: BaseNode, event: Event) -> None:
        """把事件从 source 分发给它的下游节点。"""
        targets = self.graph.downstream(source)
        if event.route_targets:
            route_targets = set(event.route_targets)
            targets = [target for target in targets if target.node_id in route_targets]
        for target in targets:
            target_event = self._build_event(event, source, target)
            self._submit(target, target_event)

    def broadcast(self, targets: Iterable[BaseNode], event: Event) -> None:
        """把同一个事件广播给指定的一组目标节点。"""
        for target in targets:
            target_event = event.fork(target.node_id)
            self._submit(target, target_event)

    def _submit(self, target: BaseNode, event: Event) -> None:
        """根据目标节点的执行模式，把任务提交到合适的执行器。"""
        if not self.running:
            return
        if target.execution_mode == ExecutionMode.CPU:
            future = self.cpu_executor.submit(target.process, event)
            future.add_done_callback(
                lambda result: self._handle_cpu_result(target, event, result)
            )
            return
        if target.execution_mode == ExecutionMode.ASYNC:
            asyncio.run_coroutine_threadsafe(
                self._execute_node_async(target, event), self.loop
            )
            return
        self.io_executor.submit(self._execute_node_sync, target, event)

    def _execute_node_sync(self, node: BaseNode, event: Event) -> None:
        """在线程池里执行同步节点。"""
        if not self.running or not node.running:
            return
        result = node.process(event)
        self._handle_result(node, event, result)

    def _handle_cpu_result(self, node: BaseNode, event: Event, future) -> None:
        """处理进程池任务完成后的返回值。"""
        if not self.running or not node.running:
            return
        self._handle_result(node, event, future.result())

    async def _execute_node_async(self, node: BaseNode, event: Event) -> None:
        """在 asyncio loop 中执行异步节点。"""
        if not self.running or not node.running:
            return
        result = await node.process_async(event)  # type: ignore[attr-defined]
        self._handle_result(node, event, result)

    def _handle_result(self, node: BaseNode, event: Event, result) -> None:
        """把节点返回值转换成后续事件传播行为。"""
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
