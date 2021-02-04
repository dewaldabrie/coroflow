import anytree
import asyncio
import concurrent.futures
import functools
import inspect
import random
from collections import defaultdict
from copy import deepcopy
from pprint import pprint
from typing import Callable, Optional


class Pipeline:
    """
    A class to link tasks together with various patterns like fan-in, fan-out, load balancer, etc.
    Queues are uses to pass data between tasks.
    The pipeline is build by recursively reversing from the leaf nodes of the tree back to the root node.
    """
    def __init__(self):
        self.nodes = {}
        self.tasks = defaultdict(list)
        self.queues = defaultdict(dict)
        self.root_coro = None
        self.root_node = None
        self.root_task = None
        self.proxy_nodes_by_task_id = {}

    def build(self, leaf_nodes=None, stop_before: str = None):
        """
        Connect the tasks in the pipeline.
        :param leaf_nodes: Used to recurse this function, nor required from top level call.
        :param stop_before: Used to recurse this function, nor required from top level call.
        :return: None
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
        :param render: Boolean for whether to print and ASCII tree representation of the dag
        :param show_queues: Boolean for whether to show the queues dictionary (for debugging)
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
            await asyncio.gather(*[task for sublist in self.tasks.values() for task in sublist])
            print("Root coro is finished.")
            # join all input queues
            for node in anytree.PreOrderIter(self.root_node):
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
        for pre, fill, node in anytree.RenderTree(self.root_node):
            print("%s%s" % (pre, node.name))


class OutputPattern:
    """
    Options for connecting pipeline tasks together:
    :param fanout: Pass the output of a task to all nodes connected to it in the DAG.
    :param lb: Pass the output of this task to the node with the shortest input queue of those connected to it.
    """
    fanout = 'fanout'  # send to every child queueu
    load_balance = 'lb'  # send to child queue with shortest queue size


class ParallelisationMethod:
    event_loop = 'event-loop'
    thread_pool = 'threads'
    process_pool = 'processes'


class Node(anytree.Node):
    """
    An extension of an Anytree Node. The tree is used as an easy way to contruct a DAG.
    This class builds a coroutine that can read data from it's input queue and submit data to it's target queue(s).
    After reading from the input queue it creates a new task to handle the data with the task logic that
    is passed in at construction/class definition.
    """
    def __init__(
            self,
            task_id,
            pipeline,
            coro_func=None,
            setup=None,
            execute=None,
            teardown=None,
            output_pattern=OutputPattern.fanout,
            parallelisation_method: ParallelisationMethod = None,
            max_concurrency=None,
            kwargs=None
    ):
        """
        Pass in the logic for your task as well as which output pattern to use for data propagation.
        :param task_id: Unique name (string) for the task
        :param pipeline: Pipeline object that the task belongs to
        :param coro_func: Custom function to use as an async task builder. Only for advanced users.
        :param setup: Setup function that passes context to the task logic in execute function
        :param execute: Node logic to handle input and generate output to next stage
        :param teardown: Teardown function to clean up context
        :param output_pattern: How to propogate data to the next stage
        :param parallelisation_method: whether to run the task in the event loop, thread- or process-pool
        :param max_concurrency: How many task instances may be active at any given time
        :param kwargs:
        """
        self.pipeline = pipeline
        if task_id in self.pipeline.nodes:
            raise ValueError(f'Node task_id must be unique, but `{task_id}` already exists.')
        self.pipeline.nodes[task_id] = self
        self._coro_func: Optional[Callable] = coro_func
        if setup:
            self.setup = setup
        if execute:
            self.execute = execute
        if teardown:
            self.teardown = teardown
        self.output_pattern = output_pattern
        self.parallelisation_method = parallelisation_method
        self.max_concurrency = max_concurrency
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
        if not hasattr(self, 'execute'):
            raise ValueError("Please supply an execute function.")

        async def coro_func(
                queues,
                task_id=self.task_id,
                **kwargs
        ):
            async def outer(target_qs, input_q, inpt, context=None):
                try:
                    async def handle_output(output):
                        if output is not None:
                            if target_qs is None:
                                pass
                            # fanout pattern
                            elif self.output_pattern == OutputPattern.fanout:
                                for target_q in target_qs:
                                    await target_q.put(output)
                            # load-balancer pattern
                            elif self.output_pattern == OutputPattern.load_balance:
                                q_sizes = [t.qsize() for t in target_qs]
                                min_q_idxs = [q for i, q in enumerate(q_sizes) if q == min(q_sizes)]
                                target_q = target_qs[random.choice(min_q_idxs)]
                                await target_q.put(output)
                            else:
                                raise ValueError("Unexpected OutputPattern %s." % self.output_pattern)

                    # Treat execute func as an async generator
                    if inspect.isasyncgenfunction(self.execute):
                        if context:
                            kwargs['context'] = context
                        if self.is_root:
                            args = []  # don't pass input to root node (no input available)
                        else:
                            args = [inpt]
                        async for output in self.execute(*args, **kwargs):
                            await handle_output(output)
                    # Treat execute func as a normal generator
                    elif inspect.isgeneratorfunction(self.execute):
                        if context:
                            kwargs['context'] = context
                        if self.is_root:
                            args = []  # don't pass input to root node (no input available)
                        else:
                            args = [inpt]
                        blocking_generator = self.execute(*args, **kwargs)

                        def catch_stop_iter(gen):
                            try:
                                res = next(gen)
                                return res, False
                            except StopIteration:
                                return None, True
                        if self.parallelisation_method == ParallelisationMethod.event_loop:
                            while True:
                                output, gen_finished = catch_stop_iter(blocking_generator)
                                if gen_finished:
                                    break
                                await handle_output(output)
                        else:
                            if self.parallelisation_method == ParallelisationMethod.process_pool:
                                pool_class = concurrent.futures.ProcessPoolExecutor
                            else:
                                # run blocking generator in thread by default
                                pool_class = concurrent.futures.ThreadPoolExecutor

                            with pool_class(max_workers=self.max_concurrency) as pool:
                                loop = asyncio.get_running_loop()
                                while True:
                                    output, gen_finished = await loop.run_in_executor(
                                        pool, catch_stop_iter, blocking_generator
                                    )
                                    if gen_finished:
                                        break
                                    await handle_output(output)
                    # Treat execute func as an async callable
                    elif inspect.iscoroutinefunction(self.execute):
                        if context:
                            kwargs['context'] = context
                        output = await self.execute(inpt, **kwargs)
                        await handle_output(output)
                    # Treat execute func as a normal callable
                    elif inspect.isfunction(self.execute) or inspect.ismethod(self.execute):
                        if context:
                            kwargs['context'] = context
                        blocking_function = functools.partial(self.execute, inpt, **kwargs)
                        if self.parallelisation_method == ParallelisationMethod.event_loop:
                            output = blocking_function()
                            await handle_output(output)
                        else:
                            if self.parallelisation_method == ParallelisationMethod.process_pool:
                                pool_class = concurrent.futures.ProcessPoolExecutor
                            else:
                                # run blocking generator in thread by default
                                pool_class = concurrent.futures.ThreadPoolExecutor

                            with pool_class(max_workers=self.max_concurrency) as pool:
                                loop = asyncio.get_running_loop()
                                output = await loop.run_in_executor(pool, blocking_function)
                            await handle_output(output)
                    else:
                        raise ValueError("Unexpected function type for task.")

                finally:
                    if input_q is not None:
                        input_q.task_done()

            input_q = queues[task_id].get('input')
            target_qs = queues[task_id]['targets']

            context = {}
            if hasattr(self, 'setup'):
                context = await self.setup()

            try:
                # First task generates its own data, so we don't need to await the previous stage
                # since there is no previous stage.
                if self.is_root:
                    task = asyncio.create_task(outer(target_qs, input_q, None, context=context))
                    self.pipeline.tasks[self.task_id].append(task)
                else:
                    while True:
                        inpt = await input_q.get()
                        # limit concurrency if required
                        if self.max_concurrency and len(list(filter(lambda t: t.done(), self.pipeline.tasks[self.task_id]))) >= self.max_concurrency:
                            while True:
                                if len(list(filter(lambda t: t.done(), self.pipeline.tasks[self.task_id]))) >= self.max_concurrency:
                                    await asyncio.sleep(0.001)
                                else:
                                    task = asyncio.create_task(outer(target_qs, input_q, inpt, context=context))
                                    self.pipeline.tasks[self.task_id].append(task)
                                    break
                        else:
                            task = asyncio.create_task(outer(target_qs, input_q, inpt, context=context))
                            self.pipeline.tasks[self.task_id].append(task)
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

    def set_upstream(self, others):
        if isinstance(others, (list, tuple, set)):
            for other in others:
                other.set_downstream(self)
        else:
            others.set_downstream(self)
