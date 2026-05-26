from __future__ import annotations

"""工作流图结构。

WorkflowGraph 只负责维护节点和节点之间的连接关系，不负责执行节点。
真正的调度、线程池、进程池和事件分发仍然由 WorkflowRuntime 负责。
"""

from typing import Dict

from .nodes import BaseNode


class WorkflowGraph:
    """维护节点图的轻量对象。"""

    def __init__(self) -> None:
        # nodes 保存 node_id 到节点对象的映射，方便按 ID 查询节点。
        self.nodes: Dict[str, BaseNode] = {}

    def add_node(self, node: BaseNode) -> BaseNode:
        """添加节点。

        如果已经存在相同 node_id 但不是同一个对象，说明图里出现了重复节点。
        """
        existing = self.nodes.get(node.node_id)
        if existing is not None and existing is not node:
            raise ValueError(f"duplicate node_id: {node.node_id}")
        self.nodes[node.node_id] = node
        return node

    def get_node(self, node_id: str) -> BaseNode:
        """根据 node_id 获取节点。"""
        try:
            return self.nodes[node_id]
        except KeyError as exc:
            raise KeyError(f"unknown node_id: {node_id}") from exc

    def connect(self, source: BaseNode, target: BaseNode) -> BaseNode:
        """连接两个节点。

        source 的输出会流向 target，同时 target 会记录 source 作为输入。
        """
        self.add_node(source)
        self.add_node(target)
        source.connect(target)
        return target

    def upstream(self, node: BaseNode | str) -> list[BaseNode]:
        """查询节点的上游节点。"""
        target = self.get_node(node) if isinstance(node, str) else node
        return list(target.input_nodes)

    def downstream(self, node: BaseNode | str) -> list[BaseNode]:
        """查询节点的下游节点。"""
        source = self.get_node(node) if isinstance(node, str) else node
        return list(source.output_nodes)
