# better-flow

`better-flow` 是一个轻量级的 Python 节点编程库。它把业务拆成节点、事件、图结构和运行时四层：

- `BaseNode` / `FunctionNode` / `AsyncFunctionNode` / `CpuNode`
- `Event`
- `WorkflowGraph`
- `WorkflowRuntime`

## 当前能力

- 支持 `sync`、`async`、`cpu` 三种执行模式
- `sync` 任务走线程池
- `async` 任务走共享的 asyncio 事件循环
- `cpu` 任务走进程池
- 支持 `trigger()` 主动触发节点
- 支持 `emit()` 从当前节点向下游传播事件
- 支持 `RouterNode` 按 `route_targets` 分流
- 支持 `TimerSource` 定时触发
- 支持 `wait_until()` 等待外部完成信号
- 支持 `stats()` 和 `pending_stats()` 查看运行状态
- 支持基础错误收集 `runtime.errors`

## 安装

```bash
pip install -e .
```

或：

```bash
pip install .
```

## 核心概念

### Event

`Event` 是节点之间传递的数据包，包含：

- `event_id`
- `source`
- `name`
- `payload`
- `created_at`
- `trace`
- `route_targets`

### Node

节点就是一个可执行单元：

```text
node.process(event) -> result
```

运行时会根据节点的 `execution_mode` 选择不同执行器。

### Runtime

`WorkflowRuntime` 负责：

- 注册节点
- 连接节点
- 启动和停止运行
- 分发事件
- 管理线程池、进程池和 asyncio loop
- 统计未完成任务

### Graph

`WorkflowGraph` 只负责维护拓扑关系，不负责执行。

## 路由规则

`RouterNode` 会把路由结果写入 `event.route_targets`。

语义如下：

- `None`：不限制，广播给所有下游
- `[]`：不传播
- `["a", "b"]`：只发给指定下游节点

运行时在进入下一跳时会清掉路由限制，避免影响后续传播。

## 示例

### 基础流程

```python
from node_flow import Event, FunctionNode, PrintSink, WorkflowRuntime


def add_one(event: Event):
    return {"value": event.payload["value"] + 1}


runtime = WorkflowRuntime(max_workers=4)

start = runtime.register(FunctionNode("start", add_one))
sink = runtime.register(PrintSink("sink"))

runtime.connect(start, sink)

runtime.start()
runtime.trigger(start, {"value": 1})
runtime.stop()
```

### 定时触发

```python
import time

from node_flow import Event, FunctionNode, PrintSink, TimerSource, WorkflowRuntime


def multiply(event: Event):
    return {"value": event.payload["count"] * 10}


runtime = WorkflowRuntime(max_workers=4)

timer = runtime.register(TimerSource("timer", interval=0.5, count_limit=3))
worker = runtime.register(FunctionNode("worker", multiply))
sink = runtime.register(PrintSink("sink"))

runtime.connect(timer, worker)
runtime.connect(worker, sink)

runtime.start()
time.sleep(2)
runtime.stop()
```

### 观察状态

```python
stats = runtime.stats()
pending = runtime.pending_stats()
```

## 已有节点

- `FunctionNode`
- `AsyncFunctionNode`
- `CpuNode`
- `FilterNode`
- `RouterNode`
- `TimerSource`
- `PrintSink`

兼容别名：

- `TimerNode = TimerSource`
- `PrintNode = PrintSink`

## 测试

```bash
python -m unittest discover -s tests
```

```bash
python -m compileall node_flow tests
```
