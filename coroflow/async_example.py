from coroflow import Task, Pipeline
import asyncio
import time
from pprint import pprint


async def generator(queues, task_id=None):
    target_qs = queues[task_id]['targets']
    for url in ['img_url_1', 'img_url_2', 'img_url_3']:
        for target_q in target_qs:
            await asyncio.sleep(1)
            await target_q.put(url)


p = Pipeline()


async def func1(queues, param=None, task_id=None):
    async def inner(targets, inpt):
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
        print(f'func1: Creating task with func1_inner, input {inpt}.')
        asyncio.create_task(inner(target_qs, inpt))


async def func2(queues, param=None, task_id=None):
    async def inner(targets, inpt):
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
        print(f'func2: Creating task with func2_inner, input {inpt}.')
        asyncio.create_task(inner(target_qs, inpt))


t0 = Task('gen', p, coro_func=generator)
t1 = Task('func1', p, coro_func=func1, kwargs={'param': 'param_t1'})
t2 = Task('func2', p, coro_func=func2, kwargs={'param': 'param_t2'})
t0.set_downstream(t1)
t1.set_downstream(t2)


# %%
async def scrape_and_process(scraper, pipeline):
    await pipeline.abuild()
    pipeline.render()
    pprint(pipeline.queues)
    await pipeline.run()

# %%
start_time = time.time()
loop = asyncio.get_event_loop()
loop.run_until_complete(scrape_and_process(generator, p))
print(f"Asynchronous duration: {time.time() - start_time}s.")

