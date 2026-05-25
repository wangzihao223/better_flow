# node-flow

`node-flow` is a lightweight event-driven node graph runtime for Python.

## Features

- Event-based node execution
- Thread pool for sync/IO work
- Process pool for CPU-bound work
- Asyncio event loop support
- Timer source support

## Install

```bash
pip install .
```

## Usage

```python
import time

from node_flow import Event, FunctionNode, PrintSink, TimerSource, WorkflowRuntime


def delay(seconds: float):
    def run(event: Event):
        time.sleep(seconds)
        return event.payload

    return run


runtime = WorkflowRuntime(max_workers=8)
timer = runtime.register(TimerSource(interval=0.5, count_limit=4))
fast = runtime.register(FunctionNode("fast_branch", delay(0.2)))
sink = runtime.register(PrintSink("sink"))

runtime.connect(timer, fast)
runtime.connect(fast, sink)

runtime.start()
time.sleep(4)
runtime.stop()
```

