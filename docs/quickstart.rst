快速开始
========

这份快速开始面向“先理解框架，再决定怎么用”的场景。
如果你只想把几个步骤串起来，直接看示例页也可以；如果你想理解它为什么
这么设计，建议先看 :doc:`concepts` 和 :doc:`runtime`。

安装
----

开发模式安装：

.. code-block:: bash

   pip install -e .

如果需要构建文档：

.. code-block:: bash

   pip install -e ".[docs]"

最小流程
--------

下面的例子创建一个入口节点和一个 sink 节点。``trigger`` 会执行入口节点，
然后把返回结果传播给下游节点。

.. code-block:: python

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

构建文档
--------

.. code-block:: bash

   sphinx-build -b html docs docs/_build/html

构建成功后，打开 ``docs/_build/html/index.html`` 查看生成结果。
