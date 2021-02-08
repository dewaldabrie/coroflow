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
