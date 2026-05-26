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

### `trigger()`

`trigger()` 用于把事件投递给指定节点自己，执行该节点的 `process()`，再继续向下游传播。

```text
runtime.trigger(node, payload)
-> node.process(event)
-> runtime 处理返回值
-> 向 node 的下游继续传播
```

### `emit()`

`emit()` 用于从 source 节点向下游发出事件，不执行 source 自己的 `process()`。

```text
runtime.emit(source, payload)
-> source 的下游节点 process(event)
```

### `WorkflowGraph`

`WorkflowGraph` 已从 runtime 中拆出来，负责图结构维护。

当前能力：

- `add_node`
- `get_node`
- `connect`
- `upstream`
- `downstream`
- 重复 `node_id` 检查

### `RouterNode` 分流

`RouterNode` 已接入 runtime 分发逻辑。

当前 route 语义：

```text
event.route_targets is None  -> 没有路由限制，发给所有下游
event.route_targets == []    -> 明确不选择任何分支，不传播
event.route_targets == [...] -> 只发给指定下游
```

`route_targets` 只影响当前这一跳。事件进入下一跳前，runtime 会把 `route_targets` 恢复为 `None`，避免路由规则污染后续传播。

### 错误处理

第一版错误处理已完成，采用“捕获、记录、停止当前分支”的策略。

当前行为：

```text
节点执行异常
-> runtime 捕获异常
-> 生成 name="error" 的 Event
-> 写入 runtime.errors
-> 当前分支停止传播
```

已接入的执行路径：

- `_execute_node_sync`
- `_execute_node_async`
- `_handle_cpu_result`

当前不会把 error event 自动投递到图中，后续如需要可以增加错误专用分支。

## 当前返回值协议

节点返回值由 runtime 统一处理：

- `None`：停止传播。
- 普通值：替换 `event.payload` 后继续传播。
- `Event`：使用完整事件继续传播。
- `list`：拆成多个事件继续传播。

## 错误处理设计草案

第一版错误处理先做“捕获、记录、停止当前分支”，不做复杂恢复。

### 默认行为

节点执行过程中发生异常时：

```text
捕获异常
-> 生成 error event
-> 放入 runtime.errors
-> 当前分支停止传播
```

默认不继续向下游传播，避免错误数据进入后续节点。

### Runtime 新增状态

计划在 `WorkflowRuntime` 中增加：

```python
self.errors: list[Event] = []
```

用于保存运行时捕获到的错误事件，方便测试、调试和后续监控。

### error event 结构

第一版先复用 `Event`，不单独增加 `ErrorEvent` 类。

建议结构：

```python
Event(
    source=node.node_id,
    name="error",
    payload={
        "node_id": node.node_id,
        "event_id": event.event_id,
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "payload": event.payload,
    },
)
```

字段含义：

- `node_id`：出错节点。
- `event_id`：原始事件 ID。
- `error_type`：异常类型。
- `error_message`：异常消息。
- `payload`：出错时的原始 payload。

### Runtime 新增方法

计划增加：

```python
def _handle_error(self, node: BaseNode, event: Event, exc: Exception) -> None:
    ...
```

所有执行路径都统一走 `_handle_error()`：

- `_execute_node_sync`
- `_execute_node_async`
- `_handle_cpu_result`

### 同步节点错误处理

```python
def _execute_node_sync(self, node: BaseNode, event: Event) -> None:
    if not self.running or not node.running:
        return
    try:
        result = node.process(event)
    except Exception as exc:
        self._handle_error(node, event, exc)
        return
    self._handle_result(node, event, result)
```

### 异步节点错误处理

```python
async def _execute_node_async(self, node: BaseNode, event: Event) -> None:
    if not self.running or not node.running:
        return
    try:
        result = await node.process_async(event)
    except Exception as exc:
        self._handle_error(node, event, exc)
        return
    self._handle_result(node, event, result)
```

### CPU 节点错误处理

```python
def _handle_cpu_result(self, node: BaseNode, event: Event, future) -> None:
    if not self.running or not node.running:
        return
    try:
        result = future.result()
    except Exception as exc:
        self._handle_error(node, event, exc)
        return
    self._handle_result(node, event, result)
```

### 第一版测试目标

至少增加一个测试：

```text
bad_node 抛异常
-> sink 不执行
-> runtime.errors 中存在一条 error event
-> error event 中记录 node_id、error_type、error_message 和原 payload
```

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

### 1. Future 管理

当前 future 提交后没有统一追踪。

计划增加：

- pending/running/completed 任务统计
- 超时
- 取消
- 优雅停止策略

### 2. 错误处理增强

后续可继续设计：

- error event 是否支持专用下游分支。
- 是否增加全局 `on_error` 回调。
- 是否支持错误重试。
- 是否支持错误恢复/fallback 节点。
- 是否支持错误策略：`ignore`、`record`、`raise`、`emit_error`。

### 3. 路由增强

后续可继续设计：

- route 返回不存在的 `node_id` 时是否报错。
- route 返回重复 `node_id` 是否去重。
- route 是否支持直接返回节点对象。
- 是否支持默认分支。
- 是否增加 strict route 模式。

### 4. Payload 复制策略

当前 `Event.fork()` 使用深拷贝，payload 也会被深度复制。

优点：

- 分支之间互不污染。
- 默认行为安全。

问题：

- payload 很大时性能开销高。
- 某些对象不能被 deepcopy，例如文件句柄、socket、锁、数据库连接等。
- 有些场景希望共享大对象引用，而不是复制。

后续计划增加显式复制策略：

```python
class PayloadCopyMode(str, Enum):
    DEEP = "deep"
    SHALLOW = "shallow"
    REFERENCE = "reference"
```

语义：

```text
DEEP      深拷贝 payload，分支完全隔离，最安全，最慢
SHALLOW   浅拷贝 payload，外层隔离，内层对象共享
REFERENCE 不拷贝 payload，所有分支共享同一个 payload，最快，但有污染风险
```

设计方向：

- `Event.fork()` 不再 `deepcopy(self)` 整个事件。
- `Event.fork()` 显式构造新 `Event`。
- 只有 `payload` 根据复制策略处理。
- `WorkflowRuntime` 提供默认 `payload_copy_mode`。

目标用法：

```python
runtime = WorkflowRuntime(payload_copy_mode=PayloadCopyMode.DEEP)
```

默认策略建议仍然使用 `DEEP`，保证早期行为安全。

后续可扩展自定义复制函数：

```python
payload_copy_func: Callable[[Any], Any] | None = None
```

需要补充测试：

- `DEEP`：一个分支修改嵌套 payload，另一个分支不受影响。
- `SHALLOW`：外层容器不同，内层对象共享。
- `REFERENCE`：多个分支拿到同一个 payload 对象。

### 5. 更多测试

还需要补充：

- `FilterNode` 阻断测试。
- `AsyncFunctionNode` 执行测试。
- `CpuNode` 执行测试。
- `TimerSource` 定时触发测试。
- `stop()` 收尾测试。

### 6. 包结构整理

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
错误处理实现 + async/cpu/timer 测试 + Future 管理
```

这几项会决定 runtime 是否能从“可跑”进入“可靠”。
