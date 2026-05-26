示例
====

本页展示仓库 ``examples/`` 目录中的完整示例。文档通过 ``literalinclude``
直接引用真实示例文件，避免示例代码和文档内容脱节。

基础流程：Timer + Router
------------------------

这个示例展示 ``TimerSource`` 定时发事件，``RouterNode`` 根据 ``count`` 奇偶
分流，最后进入同一个 sink。

运行命令：

.. code-block:: bash

   python examples/basic_flow.py

源码：

.. literalinclude:: ../examples/basic_flow.py
   :language: python
   :linenos:

线程池并行 IO
-------------

这个示例用 ``time.sleep`` 模拟三个阻塞 IO 任务。三个下游节点会通过
``ThreadPoolExecutor`` 并行执行。

运行命令：

.. code-block:: bash

   python examples/parallel_io.py

源码：

.. literalinclude:: ../examples/parallel_io.py
   :language: python
   :linenos:

asyncio 协程并发
----------------

这个示例用 ``asyncio.sleep`` 模拟三个异步 IO 任务。三个异步节点会提交到
runtime 的共享 asyncio loop。

运行命令：

.. code-block:: bash

   python examples/parallel_async.py

源码：

.. literalinclude:: ../examples/parallel_async.py
   :language: python
   :linenos:

进程池 CPU 并行
---------------

这个示例用素数计数模拟 CPU 密集任务。``CpuNode`` 会把任务提交到
``ProcessPoolExecutor``。

运行命令：

.. code-block:: bash

   python examples/parallel_cpu.py

源码：

.. literalinclude:: ../examples/parallel_cpu.py
   :language: python
   :linenos:

运行时 hook
-----------

这个示例展示如何通过 ``add_hook()`` 观察 runtime 事件，包括启动、事件创建、
任务提交、节点执行和任务完成。

运行命令：

.. code-block:: bash

   python examples/runtime_hooks.py

源码：

.. literalinclude:: ../examples/runtime_hooks.py
   :language: python
   :linenos:
