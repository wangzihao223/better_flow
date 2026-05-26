运行时说明
==========

为什么这样设计
----------------

``node-flow`` 选择“后台 asyncio loop 子线程 + 主线程投递协程”的模型，
是因为它能把三类执行资源统一起来：

- 同步 IO 走线程池
- 异步 IO 走共享 asyncio loop
- CPU 密集任务走进程池

这样做的好处是，框架可以对外保持一个很小的 API 面：

- ``start()``
- ``stop()``
- ``trigger()``
- ``emit()``
- ``connect()``
- ``register()``

用户不用在业务代码里直接管理 event loop，也不用自己协调线程、定时器和
进程池。

runtime 的核心优势
------------------

和“自己在业务代码里手工管理线程、协程和进程池”相比，runtime 的价值主要
体现在四个方面：

1. **把调度从业务里抽出来**

   业务节点只关心输入和输出，不关心自己是在线程池、协程还是进程池里跑。
   这样节点更小、更容易测试，也更容易替换。

2. **把生命周期集中管理**

   启动、停止、等待 ready、收尾和错误记录都由 runtime 统一处理，不会散落
   在每个节点里各写一份。

3. **把观察能力做成框架能力**

   runtime 统一提供 stats、pending_stats、errors 和 hook。没有 runtime 时，
   这些能力通常要在业务里临时拼装，最后很难维护一致性。

4. **把并行与分支变成图结构**

   分支、路由、广播、聚合都通过连接关系和事件传播表达，而不是靠大量 if/
   else 和手工回调嵌套。

没有 runtime 会怎样
-------------------

如果没有 runtime，通常会退回到下面这种状态：

- 每个节点自己决定怎么调线程、怎么切协程、怎么处理取消和关闭
- 分支逻辑散落在多个函数里，代码越写越像“流程脚本”
- 错误记录、状态统计、日志 hook 都要手工拼装
- 同一条流程里同时出现同步、异步和 CPU 任务时，边界会越来越乱
- 测试会变得难写，因为你很难给“流程执行”提供统一的入口和出口

所以这个项目真正的核心不是某个单独节点，而是 runtime 提供的统一执行
模型和生命周期边界。

执行模式
--------

``node-flow`` 当前支持三种执行模式：

.. list-table::
   :header-rows: 1

   * - 模式
     - 用途
     - 执行资源
   * - ``sync``
     - 普通同步任务、阻塞 IO
     - ``ThreadPoolExecutor``
   * - ``async``
     - asyncio 异步 IO
     - 后台共享 asyncio loop
   * - ``cpu``
     - CPU 密集任务
     - ``ProcessPoolExecutor``

生命周期
--------

``WorkflowRuntime`` 采用后台 asyncio loop 子线程方案。外部线程通过
``asyncio.run_coroutine_threadsafe()`` 把异步节点和 timer 任务提交到共享
loop。

第一轮生命周期加固后：

- ``start()`` 会等待 asyncio loop ready 后再返回。
- loop 在线程内部创建、绑定和运行。
- ``stop()`` 会投递 shutdown coroutine，由 loop 自己取消 pending task 并停止。
- 同一个 runtime 实例 ``stop()`` 后暂不支持再次 ``start()``。
- CPU 进程池按需创建，并在已有 CPU 节点时提前预热。

这套生命周期设计的优点是：

- runtime 的所有外部入口都很明确
- loop 生命周期和业务流程生命周期分离
- 调试时可以集中看 hook、stats 和 pending 状态
- 在单进程 Python 应用里足够简单，同时仍然保留并行能力

状态观察
--------

可以通过 ``stats()`` 和 ``pending_stats()`` 查看运行状态，也可以通过
``add_hook()`` 订阅 runtime 事件。
