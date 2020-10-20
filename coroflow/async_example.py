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

async def func1_inner(targets, inpt):
    await asyncio.sleep(1)  # simulated IO delay
    outp = inpt
    for target in targets or []:
        print(f"func1: T1 sending {outp}")
        await target.asend(outp)
        await asyncio.sleep(0)


async def func1(targets, param):
    print(f"func1: Got param: {param}")
    tasks = []
    while True:
        try:
            inpt = (yield)
        except ValueError:
            break
        print(f'func1: Creating task with func1_inner, input {inpt}.')
        tasks.append(asyncio.create_task(func1_inner(targets, inpt)))
        await asyncio.sleep(0)
    for t in tasks:
        await t
    for target in targets or []:
        await target.athrow(ValueError())


async def func2_inner(targets, inpt):
    await asyncio.sleep(1)  # simulated IO delay
    outp = inpt
    for target in targets or []:
        print(f"func2: T2 sending {outp}")
        await target.asend(outp)
        await asyncio.sleep(0)


async def func2(targets, param):
    print(f"func2: Got param: {param}")
    tasks = []
    while True:
        try:
            inpt = (yield)
        except ValueError:
            break
        print(f'func2: Creating task with func2_inner, input {inpt}.')
        tasks.append(asyncio.create_task(func2_inner(targets, inpt)))
        await asyncio.sleep(0)
    for t in tasks:
        await t
    for target in targets or []:
        await target.athrow(ValueError())



t1 = Task('foo', p, func1, args=('param_t1',))
t2 = Task('bar', p, func2, args=('param_t2',))
t1.set_downstream(t2)


# %%
async def scrape_and_process(scraper, pipeline):
    await pipeline.abuild()
    pipeline.render()
    pipeline_root = pipeline.root_task
    await asyncio.gather(*[pipeline_root.asend(url) async for url in scraper()])
    try:
        await pipeline_root.athrow(ValueError())
    except Exception as e:
        if 'StopAsyncIteration' in str(e):
            pass

# %%
start_time = time.time()
asyncio.run(scrape_and_process(async_image_scraper, p))
print(f"Asynchronous duration: {time.time() - start_time}s.")

