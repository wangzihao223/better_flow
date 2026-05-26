from __future__ import annotations

"""WorkflowGraph 的图结构维护测试。"""

import unittest

from node_flow import FunctionNode, WorkflowGraph


def noop(event):
    return event.payload


class WorkflowGraphTests(unittest.TestCase):
    def test_add_node_rejects_duplicate_node_id(self) -> None:
        """同一个图里不能添加两个不同对象但 node_id 相同的节点。"""
        graph = WorkflowGraph()
        graph.add_node(FunctionNode("node", noop))

        with self.assertRaises(ValueError):
            graph.add_node(FunctionNode("node", noop))

    def test_connect_updates_upstream_and_downstream(self) -> None:
        """connect 会同时维护出边和入边。"""
        graph = WorkflowGraph()
        source = FunctionNode("source", noop)
        target = FunctionNode("target", noop)

        graph.connect(source, target)

        self.assertEqual(graph.downstream(source), [target])
        self.assertEqual(graph.upstream(target), [source])
        self.assertEqual(graph.downstream("source"), [target])
        self.assertEqual(graph.upstream("target"), [source])


if __name__ == "__main__":
    unittest.main()
