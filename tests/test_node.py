"""
TODO:
    test concurrency limit on nodes including those with async tasks
    test context passing between setup, execute and teardown
    factor our logic to get type (async, sync, gen) of execute function and test as a unit
    factor out logic to track number of active tasks to Node class - also store tasks for a node in the node itself
"""
import asyncio
from unittest import TestCase

from coroflow import Node, Pipeline


class TestSetupTeardown(TestCase):
    def test_kwargs_to_setup_and_teardown(self):

        class GenNode(Node):
            async def execute(self):
                for i in range(3):
                    await asyncio.sleep(0.0001)
                    yield i

        class MyNode(Node):
            async def setup(self, **kwargs):
                if 'foo' in kwargs:
                    context = {'count': 1}
                    return context
                else:
                    return {}

            async def execute(self, inpt, context=None, **kwargs):
                await asyncio.sleep(0.0001)
                if not context:
                    context = {}
                return inpt + context.get('count', 0)

            async def teardown(self, context, **kwargs):
                pass

        outputs = []

        class DataCapture(Node):
            async def execute(self, inpt):
                outputs.append(inpt)

        p = Pipeline()
        t0 = GenNode('gen', p)
        t1 = MyNode('stage1', p, kwargs=dict(foo='bar'))
        t2 = DataCapture('capture', p)
        t0.set_downstream(t1)
        t1.set_downstream(t2)
        p.run()

        self.assertEqual(sorted(outputs), [1, 2, 3])


class TestConcurrencyLimits(TestCase):

    def test_async_node_concurrency_limit(self):

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

    def test_sync_node_thread_concurrency_limit(self):
        pass

    def test_sync_node_process_concurrency_limit(self):
        pass
