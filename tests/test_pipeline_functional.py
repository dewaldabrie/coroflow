import asyncio
import time
from unittest import TestCase

from coroflow import Pipeline, Node, ParallelisationMethod


def my_sync_task_execute(inpt):
    return inpt


async def agen():
    for i in range(3):
        await asyncio.sleep(0.0001)
        yield i


class TestPipelineChaining(TestCase):

    def test_chain_with_custom_coro_funcs(self):
        output = []

        async def generator(input_q, target_qs):
            for url in ['img_url_1', 'img_url_2', 'img_url_3']:
                for target_q in target_qs:
                    await asyncio.sleep(1)
                    await target_q.put(url)

        async def func1(input_q, target_qs, param=None):
            async def execute(targets, inpt):
                outp = inpt
                for target in targets or []:
                    await target.put(outp)
                nonlocal input_q
                input_q.task_done()

            while True:
                inpt = await input_q.get()
                asyncio.create_task(execute(target_qs, inpt))

        async def final(input_q, target_qs, param=None):
            async def execute(targets, inpt):
                output.append(inpt)
                nonlocal input_q
                input_q.task_done()

            while True:
                inpt = await input_q.get()
                asyncio.create_task(execute(target_qs, inpt))

        p = Pipeline()

        t0 = Node('gen', p, async_worker_func=generator)
        t1 = Node('func1', p, async_worker_func=func1, kwargs={'param': 'param_t1'})
        tf = Node('final', p, async_worker_func=final, kwargs={'param': 'param_t1'})
        t0.set_downstream(t1)
        t1.set_downstream(tf)

        p.run()

        self.assertEqual(output, ['img_url_1', 'img_url_2', 'img_url_3'])

    def test_chaining_with_task_classes(self):

        class GenNode(Node):
            async def execute(self):
                for i in range(3):
                    await asyncio.sleep(0.0001)
                    yield i

        class MyNode(Node):
            async def execute(self, inpt):
                await asyncio.sleep(0.0001)
                return inpt

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = GenNode('gen', p)
        t1 = MyNode('stage1', p)
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [0, 1, 2])

    def test_chaining_with_non_async_generator(self):

        class SyncGenNode(Node):
            def execute(self):
                for i in range(3):
                    yield i

        class MyNode(Node):
            async def execute(self, inpt):
                await asyncio.sleep(0.0001)
                return inpt

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = SyncGenNode('sync_gen', p)
        t1 = MyNode('stage1', p)
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [0, 1, 2])

    def test_chaining_with_non_async_function(self):

        class GenNode(Node):
            async def execute(self):
                for i in range(3):
                    await asyncio.sleep(0.0001)
                    yield i

        class MySyncNode(Node):
            def execute(self, inpt):
                return inpt

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = GenNode('gen', p)
        t1 = MySyncNode('synchronous_stage1', p)
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [0, 1, 2])

    def test_parralelisation_strategy_selection(self):

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = Node('gen', p, execute=agen)
        t1 = Node('synchronous_stage1', p, execute=my_sync_task_execute,
                  parallelisation_method=ParallelisationMethod.process_pool, max_concurrency=10)
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [0, 1, 2])

    def test_io_task_concurrency_time_saving(self):
        """Test that n tasks in parrallel execute in roughly the same time as 1."""
        class GenNode(Node):
            async def execute(self):
                for i in range(3):
                    await asyncio.sleep(0.0001)
                    yield i

        class MyNode(Node):
            async def execute(self, inpt):
                await asyncio.sleep(1)
                return inpt

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = GenNode('gen', p)
        conc_tasks = [MyNode(f'stage1_t{i}', p) for i in range(10)]
        t2 = DataCapture('capture', p)
        for t in conc_tasks:
            t0.set_downstream(t)
            t.set_downstream(t2)
        start = time.time()
        p.run()
        duration = time.time() - start

        self.assertLess(duration, 2)
        self.assertEqual(sorted(outputs), [0] * 10 + [1] * 10 + [2] * 10)
