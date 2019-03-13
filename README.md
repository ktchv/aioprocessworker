# aioprocessworker
Python coroutine separate process worker. Child is spawned using muliporcessing and communicate through socket/unix socket and pickled messages 

#### Starting worker
```python
from aioprocessworker import AsyncWorker 
worker = AsyncWorker()
await worker.start()
```

#### Scheduling coroutine
Use `schedule_coroutine` method. It will return Future object which could be cancelled. Once coroutine is completed Future result or exception will be set
```python
async def run_me_in_worker(param):
    print(f"I'm child run with {param}")
    await asyncio.sleep(3)
    return {"So much": "Data"}
    
res = await worker.schedule_coroutine(run_me_in_worker, 123)
print(f"Parent got {res}")

```
> I'm child run with 123

> Parent got {"So much": "Data"}

#### Communicating with parent
```python
from aioprocessworker import get_parent_process 
async def client_coro():
    parent = get_parent_process()
    if parent:
        data = await parent.schedule_coroutine(parent_coro,)
        data['some more'] = 456
        return data
    
async def parent_coro():
    await asyncio.sleep(5)
    return {"Data": 123}
    
data = await worker.schedule_coroutine(client_coro)
print(data)
```
> {'Data': 123, 'some more': 456}
#### Terminating or context manager

Call `stop()` to stop worker or use context manager:
```python
async with AsyncWorker() as worker:
    res = await worker.schedule_coroutine(client_coro)
    print(f"Result: {res}")
```

#### Logging
Turn on `AsyncWorkerServer`, `AsyncWorkerClient` and `AsyncWorkerClientMain` loggers for debug info
