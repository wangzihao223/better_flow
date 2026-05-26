# 开发日志

## 当前进度

- 已实现最小事件驱动节点运行时。
- `Event` 作为节点之间传递的数据包。
- `WorkflowGraph` 负责维护节点和节点之间的连接关系。
- `WorkflowRuntime` 负责节点注册、事件分发和执行资源管理。
- 节点不再自己管理线程、进程或事件循环。
- 已支持三种执行模式：
  - `sync`：普通同步任务，交给线程池。
  - `async`：异步任务，交给 asyncio 事件循环。
  - `cpu`：CPU 密集任务，交给进程池。
- 已有基础节点：
  - `TimerSource`
  - `FunctionNode`
  - `AsyncFunctionNode`
  - `CpuNode`
  - `FilterNode`
  - `RouterNode`
  - `PrintSink`
- 项目已包装成 Python 库，并推送到 GitHub。
- 已增加单元测试，当前测试数量：6。

## 已完成的核心能力

### 1. `trigger()`

`trigger()` 用于把事件投递给指定节点自己，执行该节点的 `process()`，再继续向下游传播。

语义：

```text
runtime.trigger(node, payload)
-> node.process(event)
-> runtime 处理返回值
-> 向 node 的下游继续传播
```

### 2. `emit()`

`emit()` 用于从 source 节点向下游发出事件，不执行 source 自己的 `process()`。

语义：

```text
runtime.emit(source, payload)
-> source 的下游节点 process(event)
```

### 3. `WorkflowGraph`

`WorkflowGraph` 已从 runtime 中拆出来，负责图结构维护。

当前能力：

- `add_node`
- `get_node`
- `connect`
- `upstream`
- `downstream`
- 重复 `node_id` 检查

### 4. `RouterNode` 分流

`RouterNode` 已接入 runtime 分发逻辑。

当前 route 语义：

```text
event.route_targets is None  -> 没有路由限制，发给所有下游
event.route_targets == []    -> 明确不选择任何分支，不传播
event.route_targets == [...] -> 只发给指定下游
```

`route_targets` 只影响当前这一跳。事件进入下一跳前，runtime 会把 `route_targets` 恢复为 `None`，避免路由规则污染后续传播。

## 当前返回值协议

节点返回值由 runtime 统一处理：

- `None`：停止传播。
- 普通值：替换 `event.payload` 后继续传播。
- `Event`：使用完整事件继续传播。
- `list`：拆成多个事件继续传播。

## 当前测试覆盖

已有测试覆盖：

- `trigger()` 会执行入口节点。
- `emit()` 不执行当前节点，只触发下游。
- `WorkflowGraph` 会维护上下游关系。
- `WorkflowGraph` 会检查重复 `node_id`。
- `RouterNode` 会只分发到指定分支。
- `RouterNode` 返回空列表时停止传播。

运行测试：

```bash
python -m unittest discover -s tests
```

## 待处理问题

### 1. 错误处理

节点执行异常现在还没有统一处理。

计划增加：

- `_handle_error`
- error event
- 基础日志
- 是否继续传播的错误策略

### 2. Future 管理

当前 future 提交后没有统一追踪。

计划增加：

- pending/running/completed 任务统计
- 超时
- 取消
- 优雅停止策略

### 3. 路由增强

后续可继续设计：

- route 返回不存在的 `node_id` 时是否报错。
- route 返回重复 `node_id` 是否去重。
- route 是否支持直接返回节点对象。
- 是否支持默认分支。
- 是否增加 strict route 模式。

### 4. 更多测试

还需要补充：

- `FilterNode` 阻断测试。
- `AsyncFunctionNode` 执行测试。
- `CpuNode` 执行测试。
- `TimerSource` 定时触发测试。
- `stop()` 收尾测试。

### 5. 包结构整理

后续可以拆成：

```text
node_flow/
  event.py
  graph.py
  runtime.py
  executors.py
  nodes/
```

## 下一阶段优先级

下一步建议优先做：

```text
错误处理 + Future 管理 + async/cpu/timer 测试
```

这几项会决定 runtime 是否能从“可跑”进入“可靠”。
