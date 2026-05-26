# better-flow

`better-flow` is a lightweight Python library for node-based workflow programming.

它的目标不是做一个庞大的工作流平台，而是提供一个小而清晰的运行时内核，
帮助你把业务流程拆成可组合、可并行、可观察的节点图。

在 `better-flow` 里，一个流程由四个核心概念组成：

- `Event`：节点之间传递的数据包
- `Node`：独立的业务执行单元
- `WorkflowGraph`：维护节点之间的拓扑关系
- `WorkflowRuntime`：统一调度、执行、传播和观察流程

项目的核心价值在 `WorkflowRuntime`。它把线程池、asyncio 事件循环、进程池、
事件分发、生命周期管理和 hook 观察能力统一到一个运行时里，让业务节点只关心
输入和输出。

## 为什么需要 better-flow

如果没有统一的 runtime，流程代码通常会逐渐变成下面的样子：

- 同步函数、协程、CPU 密集任务各自用一套调度方式
- 每个步骤自己处理线程、事件循环、取消、关闭和异常
- 分支逻辑散落在多个函数里，最后变成难维护的流程脚本
- 日志、统计、错误记录和执行状态需要临时拼装
- 测试时很难给整个流程提供统一入口和出口

`better-flow` 解决的是这个问题：把流程拆成节点，把连接关系交给图结构，把执行和
生命周期交给 runtime。

## 核心能力

- 支持 `sync`、`async`、`cpu` 三种执行模式
- `sync` 节点通过 `ThreadPoolExecutor` 执行
- `async` 节点通过共享的后台 asyncio loop 执行
- `cpu` 节点通过 `ProcessPoolExecutor` 执行
- 支持 `trigger()` 主动触发入口节点
- 支持 `emit()` 从当前节点向下游传播事件
- 支持 `RouterNode` 按 `route_targets` 分流
- 支持 `TimerSource` 定时触发事件
- 支持 `wait_until()` 等待外部完成信号
- 支持 `stats()` 和 `pending_stats()` 查看运行状态
- 支持 `add_hook()` / `remove_hook()` 订阅 runtime 事件
- 支持基础错误收集 `runtime.errors`

## 安装

开发模式安装：

```bash
pip install -e .
```

普通安装：

```bash
pip install .
```

构建文档依赖：

```bash
pip install -e ".[docs]"
```

## 最小示例

下面的例子创建一个入口节点和一个输出节点。`trigger()` 会执行入口节点，
然后 runtime 会把返回结果继续传播给下游节点。

```python
from node_flow import Event, FunctionNode, WorkflowRuntime


def add_one(event: Event):
    return {"value": event.payload["value"] + 1}


runtime = WorkflowRuntime(max_workers=4)

start = runtime.register(FunctionNode("start", add_one))
sink = runtime.register(FunctionNode("sink", lambda event: print(event.payload)))

runtime.connect(start, sink)

runtime.start()
try:
    runtime.trigger(start, {"value": 1})
finally:
    runtime.stop()
```

输出结果类似：

```text
{'value': 2}
```

## 执行模型

`better-flow` 的执行模型围绕 `WorkflowRuntime` 展开。

| 节点类型 | 适合场景 | 执行资源 |
| --- | --- | --- |
| `FunctionNode` | 普通同步任务、阻塞 IO | 线程池 |
| `AsyncFunctionNode` | asyncio 网络 IO、数据库 IO | 后台共享 asyncio loop |
| `CpuNode` | CPU 密集计算 | 进程池 |

这意味着同一条流程里可以同时出现同步节点、异步节点和 CPU 节点。业务代码不需要
直接管理线程、协程或进程池，只需要声明节点的执行方式。

## 事件和返回值协议

节点接收 `Event`，并通过返回值决定下一步如何传播：

- 返回 `None`：停止传播
- 返回普通值：替换 `event.payload` 后继续传播
- 返回 `Event`：使用完整事件继续传播
- 返回 `list`：拆成多个事件继续传播

`Event` 包含：

- `event_id`
- `source`
- `name`
- `payload`
- `created_at`
- `trace`
- `route_targets`

## 路由和分支

`RouterNode` 可以通过 `event.route_targets` 控制当前这一跳的传播目标：

- `None`：广播给所有下游节点
- `[]`：不继续传播
- `["a", "b"]`：只发给指定下游节点

runtime 在进入下一跳时会清理路由限制，避免当前路由决策污染后续传播。

## TimerSource

`TimerSource` 是一个定时事件源，适合周期性触发流程：

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
try:
    time.sleep(2)
finally:
    runtime.stop()
```

## 运行时观察

runtime 提供两类观察方式。

状态查询：

```python
stats = runtime.stats()
pending = runtime.pending_stats()
errors = runtime.errors
```

hook 订阅：

```python
def on_runtime_event(kind, data):
    print(kind, data)


runtime.add_hook(on_runtime_event)
```

hook 可以观察 runtime 启停、事件创建、事件分发、任务提交、节点开始、节点结束、
任务完成和错误记录。

## 内置节点

- `BaseNode`
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

## 示例文件

仓库的 `examples/` 目录提供了更完整的示例：

```bash
python examples/basic_flow.py
python examples/parallel_io.py
python examples/parallel_async.py
python examples/parallel_cpu.py
python examples/runtime_hooks.py
```

示例覆盖：

- `TimerSource` + `RouterNode` 基础流程
- 线程池并行 IO
- asyncio 协程并发
- 进程池 CPU 并行
- runtime hook 观察

## 文档

项目使用 Sphinx 维护文档，文档源码位于 `docs/`。

本地构建：

```bash
sphinx-build -W -b html docs docs/_build/html
```

构建后打开：

```text
docs/_build/html/index.html
```

Read the Docs 部署配置位于 `.readthedocs.yaml`。推送到 Git 托管平台后，
Read the Docs 会读取该配置并构建在线文档。

## 测试

运行单元测试：

```bash
python -m unittest discover -s tests
```

检查 Python 文件是否可以正常编译：

```bash
python -m compileall node_flow tests examples
```

## 适用场景

`better-flow` 适合：

- 在 Python 应用内部组织事件驱动流程
- 把业务步骤拆成独立、可测试、可复用的节点
- 在同一条流程中混合同步、异步和 CPU 密集任务
- 需要轻量级路由、分支、定时触发和运行时观察能力

它暂时不适合：

- 大型分布式工作流平台
- 强持久化任务编排
- 跨机器调度
- 复杂重试、补偿、断点恢复系统

## 开发状态

当前项目处于早期开发阶段，重点在 runtime 生命周期、执行模型、事件传播语义和
文档体系上。API 可能继续调整。
