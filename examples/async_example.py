from coroflow import Node, Pipeline
import asyncio
import time
from pprint import pprint


async def generator(queues, task_id=None):
    target_qs = queues[task_id]['targets']
    for url in ['img_url_1', 'img_url_2', 'img_url_3']:
        for target_q in target_qs:
            await asyncio.sleep(1)
            await target_q.put(url)


async def func1(queues, param=None, task_id=None):
    async def execute(targets, inpt):
        # do your async pipelined work
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        for target in targets or []:
            print(f"func1: T1 sending {outp}")
            await target.put(outp)
        nonlocal input_q
        input_q.task_done()

    print(f"func1: Got param: {param}")
    input_q = queues[task_id]['input']
    target_qs = queues[task_id]['targets']

    # do any setup here

    while True:
        inpt = await input_q.get()
        print(f'func1: Creating task with func1_execute, input {inpt}.')
        asyncio.create_task(execute(target_qs, inpt))


async def func2(queues, param=None, task_id=None):
    async def execute(targets, inpt):
        print(f"func2: T2 processing {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        for target in targets or []:
            await target.put(outp)
        nonlocal input_q
        input_q.task_done()

    print(f"func2: Got param: {param}")
    input_q = queues[task_id]['input']
    target_qs = queues[task_id]['targets']

    while True:
        inpt = await input_q.get()
        print(f'func2: Creating task with func2_execute, input {inpt}.')
        asyncio.create_task(execute(target_qs, inpt))


p = Pipeline()

t0 = Node('gen', p, coro_func=generator)
t1 = Node('func1', p, coro_func=func1, kwargs={'param': 'param_t1'})
t2 = Node('func2', p, coro_func=func2, kwargs={'param': 'param_t2'})
t3 = Node('func3', p, coro_func=func2, kwargs={'param': 'param_t3'})
t4 = Node('func4', p, coro_func=func2, kwargs={'param': 'param_t4'})
t0.set_downstream(t1)
t1.set_downstream(t2)
t1.set_downstream(t3)
t2.set_downstream(t4)
t3.set_downstream(t4)



# %%
start_time = time.time()
p.run()
print(f"Asynchronous duration: {time.time() - start_time}s.")

