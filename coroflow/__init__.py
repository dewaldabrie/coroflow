import asyncio
import concurrent.futures
import functools
import inspect
import types
import random
from anytree import Node, RenderTree, PreOrderIter
from collections import defaultdict
from copy import deepcopy
from pprint import pprint
from typing import Callable, Optional


class Pipeline:
    def __init__ (self):
        self.nodes = {}
        self.tasks = []
        self.queues = defaultdict(dict)
        self.root_coro = None
        self.root_node = None
        self.root_task = None
        self.proxy_nodes_by_task_id = {}

    def build(self, leaf_nodes=None, stop_before: str = None):
        """
        TODO: test with pipeline of unity length
        """
        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            leaf.coro = leaf.coro_func(None, **leaf.kwargs)
            leaf.coro.send(None)
            node = leaf
            while True:
                if node.is_root:
                    self.root_coro = node.coro
                    self.root_node = node
                    self.root_coro.send(None)
                    break
                parent = node.parent
                parent_targets = []
                children = parent.children
                for child in children:
                    if child.coro is None:
                        if child.is_leaf:
                            child.coro = child.coro_func(None, **child.kwargs)
                            child.coro.send(None)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                self.build(leaf_nodes=[descendant_leaf], stop_before=parent.name)
                    parent_targets.append(child.coro)

                if parent.name == stop_before:
                    return
                if not parent.coro:
                    parent.coro = parent.coro_func(parent_targets, **parent.kwargs)
                    parent.coro.send(None)

                node = parent

    def coro_setup(self, node, targets=None):
        """
        Prime the coroutine and pass in the input and target queues.
        """
        # If the node is a proxy for another node, skip this since we
        # don't need queues for the proxy (coroutines are shared between
        # the proxies).
        if node.is_proxy_for_node is None:
            q = asyncio.Queue() if not node.is_root else None
            self.queues[node.task_id]['input'] = q
            self.queues[node.task_id]['targets'] = targets
            node.prime(self.queues)
        else:
            # make sure the coro is shared between proxy and original
            if node.task_id not in self.queues:
                q = asyncio.Queue() if not node.is_root else None
                self.queues[node.task_id]['input'] = q
                self.queues[node.task_id]['targets'] = targets
                node.is_proxy_for_node.prime(self.queues)
            node.coro = node.is_proxy_for_node.coro

    async def abuild(self, leaf_nodes=None, stop_before: str = None):
        """
        Recursively build tree in reverse.
        """

        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            self.coro_setup(leaf)
            node = leaf
            while True:
                if node.is_root:
                    node.prime(self.queues)
                    self.root_coro = node.coro
                    self.root_node = node
                    break
                parent = node.parent
                parent_targets = []
                children = parent.children
                for child in children:
                    if child.coro is None:
                        if child.is_leaf:
                            self.coro_setup(child)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                await self.abuild(leaf_nodes=[descendant_leaf], stop_before=parent.name)
                    parent_targets.append(child.task_id)

                if parent.name == stop_before:
                    return
                if not parent.coro:
                    self.coro_setup(parent, targets=[self.queues[task_id]['input'] for task_id in parent_targets])

                node = parent

        print("Build complete.")

    def run(self, render=True, show_queues=False):
        """
        Build, then await the initial generator and then join all the input queues.
        :return:
        """
        async def _run():
            await self.abuild()
            if render:
                self.render()
            if show_queues:
                pprint(self.queues)
            # await initial generator
            await self.root_task
            await asyncio.gather(*self.tasks)
            # done, pending = await asyncio.wait([self.root_task])
            print("Root coro is finished.")
            # join all input queues
            for node in PreOrderIter(self.root_node):
                task_id = node.task_id
                input_queue = self.queues[task_id]['input']
                if input_queue is not None:  # root node has not input queue
                    pprint(f"Joining queue for {task_id}")
                    await input_queue.join()
                    pprint(f"Done joining queue for {task_id}")

        asyncio.run(_run())

    def render(self):
        """
        ASCII representation of the tree/DAG
        """
        for pre, fill, node in RenderTree(self.root_node):
            print("%s%s" % (pre, node.name))


class OutputPattern:
    fanout = 'fanout'  # send to every child queueu
    load_balance = 'lb'  # send to child queue with shortest queue size


class Task(Node):
    def __init__(
            self,
            task_id,
            pipeline,
            coro_func=None,
            setup=None,
            inner=None,
            teardown=None,
            output_pattern=OutputPattern.fanout,
            kwargs=None
    ):
        self.pipeline = pipeline
        if task_id in self.pipeline.nodes:
            raise ValueError(f'Task task_id must be unique, but `{task_id}` already exists.')
        self.pipeline.nodes[task_id] = self
        self._coro_func: Optional[Callable] = coro_func
        if setup:
            types.MethodType(setup, self)  # bind func to this class instance
        if inner:
            types.MethodType(inner, self)  # bind func to this class instance
        if teardown:
            types.MethodType(teardown, self)  # bind func to this class instance
        self.output_pattern = output_pattern
        self.coro = None
        self.kwargs = kwargs or {}
        self.targets = None
        self.is_proxy_for_node = None
        super().__init__(task_id)

    @property
    def coro_func(self):
        if self._coro_func:
            return self._coro_func
        else:
            return self.coro_func_builder()

    @coro_func.setter
    def coro_func(self, value):
        self._coro_func = value

    def coro_func_builder(self):
        if not hasattr(self, 'inner'):
            raise ValueError("Please supply an inner function.")

        async def coro_func(queues, task_id=self.task_id, **kwargs):
            async def outer(target_qs, input_q, context, inpt):
                try:
                    async def handle_output(output):
                        if output:
                            # fanout pattern
                            if self.output_pattern == OutputPattern.fanout:
                                for target_q in target_qs:
                                    await target_q.put(output)
                            # load-balancer pattern
                            elif self.output_pattern == OutputPattern.load_balance:
                                q_sizes = [t.qsize() for t in target_qs]
                                min_q_idxs = [q for i, q in enumerate(q_sizes) if q == min(q_sizes)]
                                target_q = target_qs[random.choice(min_q_idxs)]
                                await target_q.put(output)
                            else:
                                pass  # don't send anything

                    # Treat inner func as an async generator
                    if inspect.isasyncgenfunction(self.inner):
                        async for output in self.inner(context, inpt, **kwargs):
                            await handle_output(output)
                    # Treat inner func as a normal generator
                    if inspect.isgeneratorfunction(self.inner):
                        # run blocking generator in thread to keep event loop running smoothly
                        loop = asyncio.get_running_loop()
                        blocking_generator = self.inner(context, inpt, **kwargs)

                        def catch_stop_iter(gen):
                            try:
                                res = next(gen)
                                return res, False
                            except StopIteration:
                                return None, True
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            while True:
                                output, gen_finished = await loop.run_in_executor(
                                    pool, catch_stop_iter, blocking_generator
                                )
                                if gen_finished:
                                    break
                                await handle_output(output)
                    # Treat inner func as an async callable
                    elif inspect.iscoroutinefunction(self.inner):
                        output = await self.inner(context, inpt, **kwargs)
                        await handle_output(output)
                    # Treat inner func as a normal callable
                    elif inspect.isfunction(self.inner):
                        # run blocking function in thread to keep event loop running smoothly
                        with concurrent.futures.ThreadPoolExecutor() as pool:
                            loop = asyncio.get_running_loop()
                            blocking_function = functools.partial(self.inner, context, inpt, **kwargs)
                            output = await loop.run_in_executor(pool, blocking_function)
                        await handle_output(output)


                finally:
                    # nonlocal input_q
                    if input_q is not None:
                        input_q.task_done()

            input_q = queues[task_id].get('input')
            target_qs = queues[task_id]['targets']

            context = {}
            if hasattr(self, 'setup'):
                context = await self.setup()

            try:
                # First task must be a generator
                if self.is_root:
                    task = asyncio.create_task(outer(target_qs, input_q, context, None))
                    self.pipeline.tasks.append(task)
                else:
                    while True:
                        inpt = await input_q.get()
                        task = asyncio.create_task(outer(target_qs, input_q, context, inpt))
                        self.pipeline.tasks.append(task)
            finally:
                if hasattr(self, 'teardown'):
                    await self.teardown(context)

        return coro_func

    def prime(self, queues):
        """Prime the associated coroutine"""
        if not self.coro:
            self.coro = self.coro_func(queues, task_id=self.task_id, **self.kwargs)
            task = asyncio.create_task(self.coro)
            if self.is_root:
                self.pipeline.root_task = task


    @property
    def task_id(self):
        return self.name

    def set_downstream(self, others):
        # Difference between a tree and a dag is that
        # the tree can't join up branches that have diverged
        # like a DAG can.
        # To add this DAG capability, we check if the task is
        # already used somewhere, and if so, we copy it, but
        # make sure it references the same coroutine.
        if isinstance(others, (list, tuple, set)):
            others_copy = []
            for other in others:
                if other in self.root.descendants:
                    other_proxy = deepcopy(other)
                    other_proxy.coro_func = other.coro_func
                    other_proxy.is_proxy_for_node = other
                    other = other_proxy
                others_copy.append(other)
            others = others_copy

            if self.children:
                self.children += tuple(c for c in others)
            else:
                self.children = tuple(c for c in others)
        else:
            other = others
            if other in self.root.descendants:
                other_proxy = deepcopy(other)
                other_proxy.coro_func = other.coro_func
                other_proxy.is_proxy_for_node = other
                other = other_proxy

            if self.children is not None:
                self.children += (other,)
            else:
                self.children = (other,)


def coroutine(func):
    """Helps prime co-routines"""

    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        cr.send(None)
        return cr

    return start

