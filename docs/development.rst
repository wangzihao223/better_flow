开发说明
========

当前开发说明维护在仓库根目录的 ``DEVELOPMENT.md``。

重点方向
--------

- runtime 生命周期继续完善
- 禁止运行中修改图
- 增加 ``drain(timeout=None)``
- 区分立即停止和优雅停止
- 将 future 管理升级为任务表

测试命令
--------

.. code-block:: bash

   python -m unittest discover -s tests
   python -m compileall node_flow tests examples
