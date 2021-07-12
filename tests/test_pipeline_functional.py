import os
import asyncio
import time
from unittest import TestCase

from coroflow import Pipeline, Node, ParallelisationMethod


os.environ['PYTHONASYNCIODEBUG'] = "1"
os.environ['PYTHONTRACEMALLOC'] = "1"


def my_sync_task_execute(inpt, *args, **kwargs):
    return inpt


async def agen():
    for i in range(3):
        await asyncio.sleep(0.0001)
        yield i


class TestPipelineChaining(TestCase):

    def test_chaining(self):

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
                await asyncio.sleep(0.0001)

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
                await asyncio.sleep(0.1)
                return inpt

        class DataCapture(Node):
            outputs = []

            async def execute(self, inpt):
                self.outputs.append(inpt)

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

        self.assertLess(duration, 0.2)
        self.assertEqual(sorted(t2.outputs), [0] * 10 + [1] * 10 + [2] * 10)
