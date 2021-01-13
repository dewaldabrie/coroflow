from coroflow import Task, Pipeline
import asyncio
import time


class GenTask(Task):
    def execute(self, context, inpt):
        for url in ['img_url_1', 'img_url_2', 'img_url_3']:
            print(f"Yielding {url}")
            time.sleep(1)
            yield url
        print("Generator is exhausted")
        return


class Task1(Task):
    async def execute(self, context, inpt, param=None):
        # do your async pipelined work
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        print(f"func1: T1 sending {inpt}")
        return outp


class Task2(Task):
    def execute(self, context, inpt, param=None):
        print(f"func2: T2 processing {inpt}")
        time.sleep(1)  # simulated IO delay
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
p.run(render=True, show_queues=True)
print(f"Asynchronous duration: {time.time() - start_time}s.")

