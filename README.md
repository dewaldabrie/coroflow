# Coroflow: Easy and Fast Pipelines

Coroflow makes it easy to run pipelines with coroutines and also support mixing
in blocking functions and generators.

Coroflow does a lot of heavy-lifting for you:

* Manage all tasks in the pipelinen concurently in one thread using coroutines
* Pass data between tasks with queues
* Easily specify concurrency limits
* Connect stages of the pipeline with fan-out/fan-in patterns or load-balancer patterns
* Define tasks as coroutines, normal (blocking) functions, async generators or normal generators; coroflow will run it appropriately 
  in either the event-loop, a thread pool, or optionally in a processes pool
* Provides an apache-ariflow-like api for connecting tasks



## Getting Started


Coroflow makes it easy to run pipelines with coroutines and also support mixing
in blocking functions and generators

```python
    from coroflow import Node, Pipeline
    import asyncio
    import time


    class GenNode(Node):
        async def execute():
            """
            The execute method of the first/root Node has to be a generator,
            either async or synchronous.
            """
            for url in ['img_url_1', 'img_url_2', 'img_url_3']:
                print(f"Yielding {url}")
                await asyncio.sleep(1)
                yield url
            print("Generator is exhausted")
            return


    class DoSomething(Node):
        async def execute(inpt, param=None):
            """
            The execute method of all non-root Nodes should be a async
            or synchronous method.
            """
            # do your async pipelined work
            await asyncio.sleep(1)  # simulated IO delay
            outp = inpt
            print(f"func1: T1 sending {inpt}")
            return outp


    p = Pipeline()
    t0 = GenNode('gen', p)
    t1 = DoSomething('func1', p, kwargs={'param': 'param_t1'})
    t2 = DoSomething('func2', p, kwargs={'param': 'param_t2'})
    t0.set_downstream(t1)
    t1.set_downstream(t2)


    start_time = time.time()
    p.run()
    print(f"Asynchronous duration: {time.time() - start_time}s.")
```

# Tests

Run like so:

$ pytest