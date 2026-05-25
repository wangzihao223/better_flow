from __future__ import annotations

"""事件模型。

Event 是节点之间传递的数据包。Runtime 负责把 Event 从一个节点投递到
下游节点，节点只需要读取 event.payload 并返回新的处理结果。
"""

from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4
import copy
import time


@dataclass
class Event:
    # 事件唯一 ID，用于日志、追踪、去重等场景。
    event_id: str = field(default_factory=lambda: uuid4().hex)
    # 当前事件由哪个节点发出或转发。
    source: str = ""
    # 事件名称，例如 tick、message、error。
    name: str = "event"
    # 事件携带的业务数据。
    payload: Any = None
    # 事件创建时间。
    created_at: float = field(default_factory=time.time)
    # 事件已经经过的节点路径，方便调试和可视化。
    trace: list[str] = field(default_factory=list)
    # RouterNode 指定的目标节点列表。
    route_targets: list[str] = field(default_factory=list)

    def fork(self, target: str) -> "Event":
        # 分发到下游节点时复制事件，避免多个分支互相污染 payload/trace。
        cloned = copy.deepcopy(self)
        cloned.trace = [*self.trace, target]
        return cloned
