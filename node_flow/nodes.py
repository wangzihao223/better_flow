from __future__ import annotations

"""节点定义。

这一层只描述节点的行为，不负责启动线程、进程或事件循环。
Runtime 会根据 execution_mode 统一选择执行资源。
"""

from abc import ABC, abstractmethod
from enum import Enum
from inspect import isawaitable
from typing import Any, Awaitable, Callable

from .event import Event


class ExecutionMode(str, Enum):
    # 普通同步任务，通常由线程池执行。
    SYNC = "sync"
    # async 协程任务，由 asyncio 事件循环执行。
    ASYNC = "async"
    # CPU 密集任务，由进程池执行。
    CPU = "cpu"


class BaseNode(ABC):
    # 默认执行模式。
    execution_mode = ExecutionMode.SYNC

    def __init__(self, node_id: str, execution_mode: ExecutionMode | str | None = None):
        self.node_id = node_id
        self.execution_mode = ExecutionMode(execution_mode or self.execution_mode)
        self.input_nodes: list["BaseNode"] = []
        self.output_nodes: list["BaseNode"] = []
        self.runtime = None
        self.running = False

    def bind_runtime(self, runtime) -> None:
        self.runtime = runtime

    def connect(self, other: "BaseNode") -> "BaseNode":
        if other not in self.output_nodes:
            self.output_nodes.append(other)
        if self not in other.input_nodes:
            other.input_nodes.append(self)
        return other

    def start(self) -> None:
        self.running = True

    def stop(self) -> None:
        self.running = False

    def emit(self, payload: Any, name: str = "event") -> Event:
        if self.runtime is None:
            raise RuntimeError(f"{self.node_id} is not bound to runtime")
        return self.runtime.emit(self, payload, name=name)

    @abstractmethod
    def process(self, event: Event) -> Any:
        raise NotImplementedError


class TimerSource(BaseNode):
    """定时触发源。

    这个节点本身不跑循环，只描述定时器的参数，由 Runtime 统一调度。
    """

    def __init__(
        self,
        node_id: str = "timer",
        interval: float = 1.0,
        count_limit: int | None = None,
        payload_factory: Callable[[int], Any] | None = None,
    ):
        super().__init__(node_id, execution_mode=ExecutionMode.SYNC)
        self.interval = interval
        self.count_limit = count_limit
        self.payload_factory = payload_factory

    def make_payload(self, count: int) -> Any:
        if self.payload_factory is not None:
            return self.payload_factory(count)
        return {"count": count}

    def process(self, event: Event) -> Any:
        return event.payload


class FunctionNode(BaseNode):
    """同步函数节点。

    适合普通 IO 或轻量计算任务。
    """

    def __init__(
        self,
        node_id: str,
        func: Callable[[Event], Any],
        execution_mode: ExecutionMode | str = ExecutionMode.SYNC,
    ):
        super().__init__(node_id, execution_mode=execution_mode)
        self.func = func

    def process(self, event: Event) -> Any:
        return self.func(event)


class AsyncFunctionNode(BaseNode):
    """异步函数节点。

    适合基于 asyncio 的网络 IO、数据库 IO 等任务。
    """

    def __init__(self, node_id: str, func: Callable[[Event], Awaitable[Any]]):
        super().__init__(node_id, execution_mode=ExecutionMode.ASYNC)
        self.func = func

    async def process_async(self, event: Event) -> Any:
        result = self.func(event)
        if isawaitable(result):
            return await result
        return result

    def process(self, event: Event) -> Any:
        return self.func(event)


class CpuNode(FunctionNode):
    """CPU 密集节点。

    语义上和 FunctionNode 一样，但 Runtime 会把它投递到进程池。
    """

    def __init__(self, node_id: str, func: Callable[[Event], Any]):
        super().__init__(node_id, func, execution_mode=ExecutionMode.CPU)


class FilterNode(BaseNode):
    """过滤节点。

    predicate 返回 True 时才继续向下游传播。
    """

    def __init__(self, node_id: str, predicate: Callable[[Event], bool]):
        super().__init__(node_id, execution_mode=ExecutionMode.SYNC)
        self.predicate = predicate

    def process(self, event: Event) -> Any:
        if self.predicate(event):
            return event.payload
        return None


class RouterNode(BaseNode):
    """路由节点。

    根据事件内容决定后续目标节点。
    """

    def __init__(self, node_id: str, route: Callable[[Event], str | list[str]]):
        super().__init__(node_id, execution_mode=ExecutionMode.SYNC)
        self.route = route

    def process(self, event: Event) -> Event:
        routes = self.route(event)
        event.route_targets = [routes] if isinstance(routes, str) else list(routes)
        return event


class PrintSink(BaseNode):
    """调试输出节点。"""

    def __init__(self, node_id: str = "print"):
        super().__init__(node_id, execution_mode=ExecutionMode.SYNC)

    def process(self, event: Event) -> None:
        print(f"[{self.node_id}] from={event.source} trace={event.trace} payload={event.payload}")
        return None


DelayNode = FunctionNode
PrintNode = PrintSink
TimerNode = TimerSource
