from coroflow import Node, Pipeline
import asyncio
import time


class GenNode(Node):
    def execute(self):
        for url in ['img_url_1', 'img_url_2', 'img_url_3']:
            print(f"Yielding {url}")
            time.sleep(1)
            yield url
        print("Generator is exhausted")
        return


class Node1(Node):
    async def execute(self, inpt, param=None):
        # do your async pipelined work
        await asyncio.sleep(1)  # simulated IO delay
        outp = inpt
        print(f"func1: T1 sending {inpt}")
        return outp


class Node2(Node):
    def execute(self, inpt, param=None):
        print(f"func2: T2 processing {inpt}")
        time.sleep(1)  # simulated IO delay
        outp = inpt
        return outp


p = Pipeline()
t0 = GenNode('gen', p)
t1 = Node1('func1', p, kwargs={'param': 'param_t1'})
t2 = Node2('func2', p, kwargs={'param': 'param_t2'})
t0.set_downstream(t1)
t1.set_downstream(t2)


# %%
start_time = time.time()
p.run(render=True, show_queues=True)
print(f"Asynchronous duration: {time.time() - start_time}s.")

