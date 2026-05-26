# 开发日志

## 当前进度

- 已实现最小可用的事件驱动节点运行时。
- `Event` 作为节点之间传递的数据包。
- `WorkflowGraph` 负责维护节点和节点之间的连接关系。
- `WorkflowRuntime` 负责节点注册、事件分发和执行资源管理。
- 节点不直接管理线程、进程或事件循环，统一交给 runtime。
- 项目已包装成 Python 库。
- Runtime 生命周期已完成第一轮加固：`start()` 会等待 asyncio loop ready，`stop()` 会确认 loop 线程退出后再关闭 loop。
- CPU 进程池已改为按需创建，并在已有 CPU 节点时于 loop 线程启动前预热。

## 运行模型

当前 runtime 的核心执行路径：

```text
trigger(node, payload)
-> submit node
-> node.process(event)
-> runtime 处理返回值
-> dispatch 到下游节点
```

`emit(source, payload)` 不执行 `source.process()`，只从 `source` 的下游开始传播。

## 执行模式

当前支持三种执行模式：

| 模式 | 用途 | 执行资源 |
| --- | --- | --- |
| `sync` | 普通同步任务、阻塞 IO | `ThreadPoolExecutor` |
| `async` | asyncio 异步 IO | 共享 asyncio event loop |
| `cpu` | CPU 密集计算 | `ProcessPoolExecutor` |

CPU 节点目前通过顶层函数 `_run_cpu_func(func, event)` 提交到进程池，避免 Windows 下直接提交整个 node 对象时出现 pickling 问题。

## 已完成能力

### `trigger()`

`trigger()` 用于把事件投递给指定节点自身，会触发该节点的 `process()`：

```text
runtime.trigger(node, payload)
-> node.process(event)
-> 继续向 node 的下游传播
```

### `emit()`

`emit()` 用于从某个 source 节点向下游发出事件，不执行 source 自己：

```text
runtime.emit(source, payload)
-> source 的下游节点 process(event)
```

### `WorkflowGraph`

`WorkflowGraph` 已从 runtime 中拆出，负责图结构维护：

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
event.route_targets == [...] -> 只发给指定下游 node_id
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

- 同步节点
- 异步节点
- CPU 节点

当前不会把 error event 自动投递到图中。后续如果需要，可以增加错误专用分支或全局错误 hook。

### TimerSource

`TimerSource` 已从“每个 timer 一个线程”改为“所有 timer 共享 runtime 的 asyncio loop”。

当前行为：

- `TimerSource` 本身只保存 tick 配置。
- runtime 在 `start()` 时为每个 `TimerSource` 创建一个 asyncio timer task。
- timer task 使用 `await asyncio.sleep(interval)` 控制 tick 间隔。
- timer 到点后调用 `emit(node, payload, name="tick")` 把事件发给下游。
- `stop()` 会取消 timer task，并在关闭 loop 前等待取消完成。

设计原则：

```text
TimerSource 只负责 tick，不做重活。
真正的 IO / CPU 工作交给下游节点。
```

### Future 管理

第一版 Future 管理已完成。

当前能力：

- runtime 会追踪提交到线程池的 sync future。
- runtime 会追踪提交到 asyncio loop 的 async future。
- runtime 会追踪提交到进程池的 cpu future。
- future 完成后会自动从追踪集合中移除。
- `pending_count()` 返回当前未完成任务总数。
- `pending_stats()` 返回不同执行器上的未完成任务数。

`pending_stats()` 返回结构：

```python
{
    "sync_pending": 0,
    "cpu_pending": 0,
    "async_pending": 0,
    "timer_pending": 0,
    "total_pending": 0,
}
```

注意：当前 `total_pending` 只统计 sync、cpu、async 三类业务任务，不把 timer task 算入业务 pending。

### 运行状态查询

`WorkflowRuntime.stats()` 已加入，用于查看当前运行状态。

当前包含：

- `running`
- `node_count`
- `pending_count`
- `pending_stats`
- `error_count`
- `timer_count`
- `cpu_count`
- `loop_thread_alive`
- `psutil_available`

如果安装了 `psutil`，还会额外返回：

- `pid`
- `cpu_percent`
- `memory_rss`
- `thread_count`
- `cpu_num`
- `cpu_affinity`

### 等待业务完成

`wait_until(done_event, timeout=None)` 已加入 runtime。

示例里可以用 `threading.Event()` 表示业务完成信号：

```python
done = threading.Event()

runtime.start()
runtime.trigger(start, payload)
runtime.wait_until(done, timeout=5)
runtime.stop()
```

这样主线程可以等待真实完成信号，而不是用 `time.sleep()` 猜执行时间。

### Runtime 事件 hook / observer

runtime hook 已经加上，外部可以通过 `add_hook()` / `remove_hook()` 订阅运行时事件。

当前可观察事件包括：

- `runtime_started`
- `runtime_stopping`
- `runtime_stopped`
- `event_created`
- `event_dispatched`
- `task_submitted`
- `node_started`
- `node_finished`
- `node_error`
- `task_done`
- `timer_tick`

目标：

```text
runtime 一边运行，一边把内部状态变化通知给外部观察者。
```

hook 内部异常不会打断 runtime，runtime 会忽略该 hook 的异常并继续通知其他 hook。

## 当前返回值协议

节点返回值由 runtime 统一处理：

- `None`：停止传播
- 普通值：替换 `event.payload` 后继续传播
- `Event`：使用完整事件继续传播
- `list`：拆成多个事件继续传播

## 当前示例

- `examples/basic_flow.py`：基础流程、TimerSource、RouterNode、wait_until
- `examples/parallel_io.py`：线程池并行 IO 示例
- `examples/parallel_async.py`：asyncio 协程并行示例
- `examples/parallel_cpu.py`：进程池 CPU 并行示例，包含 `runtime.stats()` 输出

## 当前测试覆盖

当前单元测试数量：19。

已覆盖：

- `trigger()` 会执行入口节点
- `emit()` 不执行当前节点，只触发下游
- `WorkflowGraph` 维护上下游关系
- `WorkflowGraph` 检查重复 `node_id`
- `RouterNode` 只分发到指定分支
- `RouterNode` 返回空列表时停止传播
- 同步节点异常会进入 `runtime.errors`，并停止当前分支
- `AsyncFunctionNode` 成功执行并继续传播
- `AsyncFunctionNode` 异常会进入 `runtime.errors`
- `CpuNode` 成功执行并继续传播
- `CpuNode` 异常会进入 `runtime.errors`
- `TimerSource` 按 `count_limit` 触发指定次数
- `TimerSource` 共享 asyncio loop 后仍能正常触发
- `FilterNode` 放行 True 条件并阻断 False 条件
- `pending_count()` 追踪同步任务
- `pending_count()` 追踪异步任务
- `wait_until()` 等待业务完成信号
- `stats()` 查看运行状态、pending 数量和资源信息
- `add_hook()` 可以观察任务生命周期和事件分发
- hook 内部异常不会打断 runtime

运行测试：

```bash
python -m unittest discover -s tests
```

## 待处理问题

### 0. Runtime 生命周期加固（已完成第一轮）

当前继续采用“后台 asyncio loop 子线程 + 其他线程通过
`asyncio.run_coroutine_threadsafe()` 投递协程”的方案。

第一轮已完成的改动：

- `start()` 增加 loop ready 同步，async 节点和 `TimerSource` 不再抢在 loop ready 前提交。
- asyncio loop 改为在线程内部创建、绑定并运行，避免在主线程创建后交给子线程运行。
- `stop()` 改为投递 `_shutdown_loop()` coroutine，由 loop 自己取消 pending task 并调用 `loop.stop()`。
- `stop()` 会确认 loop 线程退出后再关闭 executor；如果 loop 线程未退出，会抛出清晰异常。
- loop 使用标准 asyncio 公开 API：`run_forever()`、`run_coroutine_threadsafe()` 和 shutdown coroutine。
- 同一个 runtime 实例 `stop()` 后暂不支持 restart，第二次 `start()` 会抛出清晰异常。
- `ProcessPoolExecutor` 改为按需创建，不用 CPU 节点时不再持有进程池。
- 如果图中已有 CPU 节点，`start()` 会在 loop 子线程启动前预热 CPU 进程池，避免多线程后再 fork。
- CPU submit 阶段异常会进入 `runtime.errors`，并发出 `node_error` / `task_done(success=False)` hook。
- 已补充生命周期测试：loop ready、stop 后 loop 关闭、禁止 restart、CPU submit 失败记录。

验证情况：

- 非沙箱环境 `python -m unittest discover -s tests` 通过，当前 24 个测试。
- `python -m compileall node_flow tests` 通过。
- `git diff --check` 通过。

当前保留的问题：

- Codex 沙箱环境下 `call_soon_threadsafe()` 唤醒 selector 不稳定，但非沙箱环境完整测试通过。
- 主实现不为沙箱限制引入私有 asyncio API、condition 命令泵或 heartbeat 兜底。
- `stop()` 语义仍然偏“停止 runtime”，不是“等待当前业务全部跑完后再关闭”。
- `register()` / `connect()` 运行中修改图还没有禁止，也没有并发安全设计。
- 如果运行中才注册 CPU 节点，会绕过 start 前 CPU pool 预热；后续建议禁止运行中改图。
- shutdown 阶段异常目前没有结构化记录，后续可以保存为 `_shutdown_error` 或进入 `runtime.errors`。

下一步建议：

1. 禁止 runtime 运行中 `register()` / `connect()` 修改图。
2. 增加 `drain(timeout=None)`，等待业务 pending 归零。
3. 增加 `stop(graceful=True)` 或等价 API，明确区分立即停止和优雅停止。
4. 将 future 管理升级为任务表，为任务 ID、取消、超时和详情统计做准备。

### 1. Runtime hook 增强

第一版 hook 已完成，后续可以继续增强：

- 增加结构化 hook event 类型
- 给任务分配稳定 task_id
- 在 hook data 中加入 pending 快照
- 支持按事件类型订阅
- 支持默认日志 hook

第二版建议把 hook 从“全局回调”升级成“按事件类型订阅”：

```text
runtime.add_hook(global_hook)      # 监听全部事件
runtime.on("node_error", hook)     # 只监听某一类事件
runtime.on("task_done", hook)
runtime.off("node_error", hook)
```

设计方向：

- 保留全局 hook，适合统一日志和调试
- 增加按事件类型的 hook，适合精细监听
- hook 回调尽量只接收本事件的数据，不再要求手动判断 kind
- 维持现有 hook 不破坏，向后兼容

### 2. Future 管理增强

当前 Future 管理只追踪未完成任务数量。

后续计划增加：

- running / completed / failed 统计
- 任务 ID
- 超时
- 取消
- 优雅停止策略

### 3. 错误处理增强

后续可以继续设计：

- error event 是否支持专用下游分支
- 是否增加全局 `on_error` 回调
- 是否支持错误重试
- 是否支持 fallback 节点
- 是否支持错误策略：`ignore`、`record`、`raise`、`emit_error`

### 4. 路由增强

后续可以继续设计：

- route 返回不存在的 `node_id` 时是否报错
- route 返回重复 `node_id` 是否去重
- route 是否支持直接返回节点对象
- 是否支持默认分支
- 是否增加 strict route 模式

### 5. Payload 复制策略

当前 `Event.fork()` 使用 `deepcopy(self)`，payload 也会被深度复制。

优点：

- 分支之间互不污染
- 默认行为安全

问题：

- payload 很大时性能开销高
- 某些对象不能被 deepcopy，例如文件句柄、socket、锁、数据库连接
- 有些场景希望共享大对象引用

后续计划增加显式复制策略：

```python
class PayloadCopyMode(str, Enum):
    DEEP = "deep"
    SHALLOW = "shallow"
    REFERENCE = "reference"
```

建议默认仍然使用 `DEEP`，保证早期行为安全。

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

## 下一阶段建议

下一步优先做 runtime hook / observer。

原因：当前已经能跑、能并行、能分流、能统计，但实时可观察性还不够。hook 做完后，runtime 才能更自然地接控制台监控、日志输出和后续 UI。
