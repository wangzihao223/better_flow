# 开发日志

## 当前进度

- 已实现最小事件驱动节点运行时。
- `Event` 作为节点之间传递的数据包。
- `WorkflowRuntime` 负责节点注册、连线、事件分发和执行资源管理。
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

## 当前语义

- `runtime.start()` 只打开 runtime 和 node 的运行状态。
- 普通节点不会在 `start()` 时自动执行。
- `emit(source, payload)` 表示 source 节点向下游发出事件，不会执行 source 自己的 `process()`。
- 节点真正执行发生在事件到达时，由 runtime 调用 `node.process(event)`。
- 节点返回值由 runtime 统一处理：
  - `None`：停止传播。
  - 普通值：替换 `event.payload` 后继续传播。
  - `Event`：使用完整事件继续传播。
  - `list`：拆成多个事件继续传播。

## 下一阶段计划

### 1. 增加 `trigger()`

解决普通节点作为入口节点的问题。

- `emit()`：从当前节点向下游发事件，不执行当前节点。
- `trigger()`：把事件投递给指定节点自己，执行该节点的 `process()`，再继续向下游传播。

目标调用方式：

```python
runtime.trigger(start_node, {"name": "hello"})
```

### 2. 增加 `WorkflowGraph`

把图结构从 runtime 中拆出来。

第一阶段能力：

- `add_node`
- `connect`
- 查询上游节点
- 查询下游节点
- 检查重复 `node_id`

后续能力：

- 环检测
- 孤立节点检查
- 拓扑排序
- JSON 导出/导入
- 可视化支持

### 3. 修正 `RouterNode` 分流

当前 `RouterNode` 只会把目标节点写入 `event.route_targets`，runtime 还没有真正按这个字段过滤下游。

目标行为：

- 如果 `event.route_targets` 为空，默认发给所有下游节点。
- 如果 `event.route_targets` 不为空，只发给指定的下游节点。

### 4. 增加错误处理

节点执行异常不能静默失败。

计划增加：

- `_handle_error`
- error event
- 基础日志
- 是否继续传播的错误策略

### 5. 增加 Future 管理

当前 future 提交后没有统一追踪。

计划增加：

- pending/running/completed 任务统计
- 超时
- 取消
- 优雅停止策略

### 6. 增加测试

至少覆盖：

- `trigger()` 会执行入口节点。
- `emit()` 不执行当前节点，只触发下游。
- `RouterNode` 正确分流。
- `FilterNode` 正确阻断。
- `AsyncFunctionNode` 正常执行。
- `CpuNode` 正常执行。
- `stop()` 能正常收尾。

### 7. 整理包结构

后续可以拆成：

```text
node_flow/
  event.py
  node.py
  graph.py
  runtime.py
  executors.py
  nodes/
```

## 优先级

下一步优先做：

```text
trigger() + WorkflowGraph + RouterNode 真分流
```

这三个属于框架核心语义，优先级高于扩展更多节点类型。
