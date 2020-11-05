from coroflow import Task, Pipeline
import asyncio
import time
from pprint import pprint


class GenTask(Task):
    @staticmethod
    async def inner(context, inpt):
        for url in ['img_url_1', 'img_url_2', 'img_url_3']:
            print(f"Yielding {url}")
            await asyncio.sleep(1)
            yield url
        print("Generator is exhausted")
        return


class Task1(Task):
    @staticmethod
    async def inner(context, inpt, param=None):
        # do your async pipelined work
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        print(f"func1: T1 sending {inpt}")
        return outp


class Task2(Task):
    @staticmethod
    async def inner(context, inpt, param=None):
        print(f"func2: T2 processing {inpt}")
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        return outp



p = Pipeline()
t0 = GenTask('gen', p)
t1 = Task1('func1', p, kwargs={'param': 'param_t1'})
t2 = Task2('func2', p, kwargs={'param': 'param_t2'})
t0.set_downstream(t1)
t1.set_downstream(t2)


# %%
start_time = time.time()
p.run()
print(f"Asynchronous duration: {time.time() - start_time}s.")
