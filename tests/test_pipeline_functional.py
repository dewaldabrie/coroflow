import asyncio
from unittest import (
    TestCase,
)

from coroflow import Pipeline, Task


class TestPipelineChaining(TestCase):

    def test_chain_with_custom_coro_funcs(self):
        output = []

        async def generator(queues, task_id=None):
            target_qs = queues[task_id]['targets']
            for url in ['img_url_1', 'img_url_2', 'img_url_3']:
                for target_q in target_qs:
                    await asyncio.sleep(1)
                    await target_q.put(url)

        async def func1(queues, param=None, task_id=None):
            async def inner(targets, inpt):
                outp = inpt
                for target in targets or []:
                    await target.put(outp)
                nonlocal input_q
                input_q.task_done()

            input_q = queues[task_id]['input']
            target_qs = queues[task_id]['targets']

            while True:
                inpt = await input_q.get()
                asyncio.create_task(inner(target_qs, inpt))

        async def final(queues, param=None, task_id=None):
            async def inner(targets, inpt):
                output.append(inpt)
                nonlocal input_q
                input_q.task_done()

            input_q = queues[task_id]['input']
            target_qs = queues[task_id]['targets']

            while True:
                inpt = await input_q.get()
                asyncio.create_task(inner(target_qs, inpt))

        p = Pipeline()

        t0 = Task('gen', p, coro_func=generator)
        t1 = Task('func1', p, coro_func=func1, kwargs={'param': 'param_t1'})
        tf = Task('final', p, coro_func=final, kwargs={'param': 'param_t1'})
        t0.set_downstream(t1)
        t1.set_downstream(tf)

        p.run()

        self.assertEqual(output, ['img_url_1', 'img_url_2', 'img_url_3'])

    def test_chaining_with_task_classes(self):

        class GenTask(Task):
            @staticmethod
            async def inner(context, inpt):
                for i in range(3):
                    yield i

        class MyTask(Task):
            @staticmethod
            async def inner(context, inpt):
                await asyncio.sleep(0.0001)
                return inpt

        outputs = []
        class DataCapture(Task):
            @staticmethod
            async def inner(context, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = GenTask('gen', p)
        t1 = MyTask('stage1', p)
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [0, 1, 2])
