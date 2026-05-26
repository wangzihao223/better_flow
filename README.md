# better-flow

`better-flow` 是一个轻量级的 Python 事件驱动节点编程库。

当前版本还处于早期开发阶段，核心目标是把节点、事件、图结构和运行时调度拆清楚：

- `Node`：处理单元，可以理解成一个函数。
- `Event`：节点之间传递的数据包，可以理解成函数参数。
- `WorkflowGraph`：维护节点和连线关系。
- `WorkflowRuntime`：负责调度节点执行，统一管理线程池、进程池和 asyncio 事件循环。

## 当前功能

- 事件驱动的节点执行模型。
- 支持节点图连线：`runtime.connect(source, target)`。
- 支持手动触发入口节点：`runtime.trigger(node, payload)`。
- 支持从某个节点向下游发事件：`runtime.emit(source, payload)`。
- 支持同步节点、异步节点和 CPU 密集节点。
- 支持定时事件源 `TimerSource`。
- 支持基础图结构 `WorkflowGraph`：
  - 添加节点
  - 连接节点
  - 查询上游
  - 查询下游
  - 重复 `node_id` 检查

## 安装

本地开发安装：

```bash
pip install -e .
```

普通本地安装：

```bash
pip install .
```

## 基本概念

### Node

节点是实际处理事件的对象。

可以先把节点理解成：

```text
node.process(event) ~= function(event)
```

节点收到事件后执行 `process()`，然后把返回值交给 runtime 处理。

### Event

`Event` 是节点之间传递的数据包，包含：

- `event_id`：事件 ID
- `source`：来源节点
- `name`：事件名称
- `payload`：业务数据
- `trace`：事件经过的节点路径
- `route_targets`：路由目标，后续用于 RouterNode 分流

### Runtime

`WorkflowRuntime` 是调度中心，负责：

- 注册节点
- 连接节点
- 启动/停止运行时
- 分发事件
- 根据节点执行模式选择执行器

执行模式：

| 模式 | 用途 | 执行器 |
| --- | --- | --- |
| `sync` | 普通同步任务、阻塞 IO | `ThreadPoolExecutor` |
| `async` | asyncio 异步 IO | asyncio event loop |
| `cpu` | CPU 密集计算 | `ProcessPoolExecutor` |

## trigger 和 emit 的区别

### trigger

`trigger()` 会执行指定节点自己的 `process()`。

```python
runtime.trigger(start_node, {"value": 1})
```

语义：

```text
执行 start_node.process(event)
然后把结果继续发给 start_node 的下游节点
```

### emit

`emit()` 不会执行 source 节点自己的 `process()`，只会从 source 的下游开始传播。

```python
runtime.emit(source_node, {"value": 1})
```

语义：

```text
source_node 产生了一个事件
runtime 把事件发给 source_node 的下游节点
```

所以：

```text
trigger = 触发当前节点自己
emit    = 从当前节点向下游发事件
```

## 示例：普通节点作为入口

```python
import time

from node_flow import Event, FunctionNode, PrintSink, WorkflowRuntime


def add_one(event: Event):
    return {"value": event.payload["value"] + 1}


runtime = WorkflowRuntime(max_workers=4)

start = runtime.register(FunctionNode("start", add_one))
sink = runtime.register(PrintSink("sink"))

runtime.connect(start, sink)

runtime.start()
runtime.trigger(start, {"value": 1})
time.sleep(0.2)
runtime.stop()
```

执行流程：

```text
trigger(start)
-> start.process(event)
-> sink.process(event)
```

## 示例：TimerSource 定时触发

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

执行流程：

```text
TimerSource 由 runtime 定时 emit
-> worker.process(event)
-> sink.process(event)
```

## WorkflowGraph

`WorkflowGraph` 只负责维护图结构，不负责执行。

```python
from node_flow import FunctionNode, WorkflowGraph


def noop(event):
    return event.payload


graph = WorkflowGraph()

a = graph.add_node(FunctionNode("a", noop))
b = graph.add_node(FunctionNode("b", noop))

graph.connect(a, b)

assert graph.downstream("a") == [b]
assert graph.upstream("b") == [a]
```

一般使用时不需要手动创建 `WorkflowGraph`，因为 `WorkflowRuntime` 内部已经维护了一个：

```python
runtime = WorkflowRuntime()
runtime.graph
```

## 当前已有节点

- `FunctionNode`：同步函数节点。
- `AsyncFunctionNode`：异步函数节点。
- `CpuNode`：CPU 密集节点。
- `FilterNode`：过滤节点。
- `RouterNode`：路由节点，目前只记录 `route_targets`，runtime 分流逻辑还未完全接上。
- `TimerSource`：定时事件源。
- `PrintSink`：调试输出节点。

兼容别名：

- `TimerNode = TimerSource`
- `PrintNode = PrintSink`

## 测试

运行单元测试：

```bash
python -m unittest discover -s tests
```

编译检查：

```bash
python -m compileall node_flow tests
```

## 下一步计划

短期优先级：

1. 修正 `RouterNode` 真正按 `route_targets` 分流。
2. 增加错误处理。
3. 增加 Future 管理。
4. 补充 async/cpu 节点测试。
5. 整理包结构和文档示例。
