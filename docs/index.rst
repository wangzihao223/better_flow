node-flow 文档
==============

``node-flow`` 是一个轻量级的 Python 事件驱动节点图运行时。

它的目标不是提供一个庞大的工作流平台，而是用尽量少的概念，把“一个
业务流程如何拆成可组合、可并行、可观察的节点”这件事讲清楚并落地。
框架把流程拆成事件、节点、图结构和运行时四层，由 ``WorkflowRuntime``
统一调度同步、异步和 CPU 密集任务。

在这个框架里，``runtime`` 不是辅助层，而是核心层。它负责：

- 统一管理节点的启动、停止和执行生命周期
- 统一调度线程池、asyncio loop 和进程池
- 统一处理事件传播、路由、分支和并行
- 统一暴露统计、错误记录和 hook 观察
- 统一把“流程”从业务代码里剥离出来

如果没有 runtime，节点之间就会变成一堆零散的函数调用、线程创建、
事件循环管理和状态传递，流程逻辑会散落在各个地方，难以复用、难以观察，
也难以保证同步、异步和 CPU 任务共存时的行为一致。

这套设计适合以下场景：

- 需要把业务步骤拆成清晰的独立节点
- 需要把同步、异步和 CPU 任务放在同一条流程里调度
- 需要分支、路由、定时触发和运行时观察能力
- 需要一个简单、可嵌入、容易读懂的 Python 节点流模型

它刻意保持小而直，不追求重型编排系统那种复杂度。

.. toctree::
   :maxdepth: 2
   :caption: 使用指南

   quickstart
   concepts
   runtime
   examples

.. toctree::
   :maxdepth: 2
   :caption: API 参考

   api

.. toctree::
   :maxdepth: 1
   :caption: 开发

   development

快速定位
--------

- 想先跑起来：阅读 :doc:`quickstart`
- 想理解设计：阅读 :doc:`concepts`
- 想看为什么这样设计：阅读 :doc:`runtime`
- 想查类和方法：阅读 :doc:`api`
- 想看完整示例：阅读 :doc:`examples`
- 想看当前计划：阅读 :doc:`development`
