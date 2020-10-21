"""
TODO this needs buffering to speed up vs synchronous counterpart.
See https://pypi.org/project/asyncio-buffered-pipeline/
"""
from coroflow import Task, Pipeline
import asyncio
import time


async def async_image_scraper():
    yield 'img_url_1'
    await asyncio.sleep(0)
    yield 'img_url_2'
    await asyncio.sleep(0)
    yield 'img_url_3'
    await asyncio.sleep(0)


p = Pipeline()


async def func1(queues, param):
    async def func1_inner(targets, inpt):
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        for target in targets or []:
            print(f"func1: T1 sending {outp}")
            await target.put(outp)
        nonlocal input_q
        input_q.task_done()

    print(f"func1: Got param: {param}")
    input_q = queues[func1.__qualname__]['input']
    target_qs = queues[func1.__qualname__]['targets']

    while True:
        inpt = await input_q.get()
        print(f'func1: Creating task with func1_inner, input {inpt}.')
        asyncio.create_task(func1_inner(target_qs, inpt))


async def func2(queues, param):
    async def func2_inner(targets, inpt):
        await asyncio.sleep(2)  # simulated IO delay
        outp = inpt
        for target in targets or []:
            print(f"func2: T2 sending {outp}")
            await target.put(outp)
        nonlocal input_q
        input_q.task_done()

    print(f"func2: Got param: {param}")
    input_q = queues[func2.__qualname__]['input']
    target_qs = queues[func2.__qualname__]['targets']

    while True:
        inpt = await input_q.get()
        print(f'func2: Creating task with func2_inner, input {inpt}.')
        asyncio.create_task(func2_inner(target_qs, inpt))


t1 = Task('foo', p, func1, args=('param_t1',))
t2 = Task('bar', p, func2, args=('param_t2',))
t1.set_downstream(t2)


# %%
async def scrape_and_process(scraper, pipeline):
    await pipeline.abuild()
    pipeline.render()
    async_generator = scraper()
    await pipeline.run(async_generator)

# %%
start_time = time.time()
loop = asyncio.get_event_loop()
loop.run_until_complete(scrape_and_process(async_image_scraper, p))
print(f"Asynchronous duration: {time.time() - start_time}s.")

