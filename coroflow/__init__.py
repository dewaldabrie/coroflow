"""
.. module:: coroflow
   :platform: Unix, Windows
   :synopsis: Easy pipelines managed by coroutines.

.. moduleauthor:: Dewald Abrie <dewaldabrie@gmail.com>
"""

import anytree
import asyncio
import concurrent.futures
import functools
import logging
import inspect
import random
from collections import defaultdict
from contextlib import contextmanager
from copy import deepcopy
from pprint import pprint


class Pipeline:
    """
    A class to link tasks together with various patterns like fan-in, fan-out, load balancer, etc.
    Queues are uses to pass data between tasks.
    The pipeline is build by recursively reversing from the leaf nodes of the tree back to the root node.

    Example:

    >>> p = Pipeline()

    """

    def __init__(self):
        self.nodes = {}
        self.workers = {}
        self.tasks = defaultdict(list)
        self.queues = defaultdict(dict)
        self.root_coro = None
        self.root_node = None
        self.root_worker = None
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
            leaf.coro = leaf.async_worker_func(None, **leaf.kwargs)
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
                            child.coro = child.async_worker_func(None, **child.kwargs)
                            child.coro.send(None)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                self.build(leaf_nodes=[descendant_leaf], stop_before=parent.task_id)
                    parent_targets.append(child.coro)

                if parent.task_id == stop_before:
                    return
                if not parent.coro:
                    parent.coro = parent.async_worker_func(parent_targets, **parent.kwargs)
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
            node.prime(q, targets)
        else:
            # make sure the coro is shared between proxy and original
            if node.task_id not in self.queues:
                q = asyncio.Queue() if not node.is_root else None
                self.queues[node.task_id]['input'] = q
                self.queues[node.task_id]['targets'] = targets
                node.is_proxy_for_node.prime(q, targets)
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
                    node.prime(None, self.queues[node.task_id]['targets'])
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
                                await self.abuild(leaf_nodes=[descendant_leaf], stop_before=parent.task_id)
                    parent_targets.append(child.task_id)

                if parent.task_id == stop_before:
                    return
                if not parent.coro:
                    self.coro_setup(parent, targets=[self.queues[task_id]['input'] for task_id in parent_targets])

                node = parent

        logging.info("Build complete.")

    def run(self, render=True, show_queues=False):
        """
        Build tree in reverse from leaves towards root,
        then await the initial generator and then join all the input queues.

        :param render: Boolean for whether to print and ASCII tree representation of the dag
        :param show_queues: Boolean for whether to show the queues dictionary (for debugging)
        :return: None
        """
        async def _run():
            await self.abuild()
            if render:
                self.render()
            if show_queues:
                pprint(self.queues)
            # await initial generator
            await self.root_worker
            # wait for all worker's tasks to be finished
            await asyncio.gather(*self.workers.values())
            # wait for all jobs to finish
            for task_set in self.tasks.values():
                await asyncio.gather(*task_set)
            # cancel all workers here
            logging.info("Root coro is finished.")
            for task_id, worker in self.workers.items():
                worker.cancel()

        asyncio.run(_run())

    def render(self):
        """
        ASCII representation of the tree/DAG
        """
        for pre, fill, node in anytree.RenderTree(self.root_node):
            print("%s%s" % (pre, node.task_id))

    @classmethod
    def simple_pipe(cls, exec_func_list):
        """
        Produce a pipeline with Nodes as simple pipe of nodes with exec funcs as passed.
        Pipe must be non-fanning.
        """
        p = cls()
        nodes = []
        for i, f in enumerate(exec_func_list):
            node_id = 'node-{}'.format(i)
            if isinstance(f, tuple):
                assert len(f) == 3, 'Expected (setup, execute, teardown) in tuple.'
                setup, execute, teardown = f
                node = Node(node_id, p, setup=setup, execute=execute, teardown=teardown)
            else:
                node = Node(node_id, p, execute=f)

            if len(nodes) > 0:
                nodes[-1].set_downstream(node)
            nodes.append(node)

        return p




class OutputPattern:
    """
    Options for connecting pipeline tasks together:

    :param fanout: Pass the output of a task to all nodes connected to it in the DAG.
    :param lb: Pass the output of this task to the node with the shortest input queue of those connected to it.
    """
    fanout = 'fanout'  # send to every child queueu
    load_balance = 'lb'  # send to child queue with shortest queue size


class ParallelisationMethod:
    """
    Options for parralelising tasks in a Node:

    :param 'event-loop': Run the function (async or sync) in the event loop. In case of sync this is not advisable
                         since it will block the event loop.
    :param 'threads': Run in a thread pool
    :param 'processes': Run in a process pool
    """
    event_loop = 'event-loop'
    thread_pool = 'threads'
    process_pool = 'processes'


class BaseOutputPatternMixin:
    @staticmethod
    async def handle_output(output, target_qs):
        raise NotImplementedError("Implement me for the relevant OutputPattern used.")


class FanoutPatternMixin(BaseOutputPatternMixin):
    @staticmethod
    async def handle_output(output, target_qs):
        if target_qs is None:
            return
        for target_q in target_qs:
            await target_q.put(output)


class LoadBalancerPatternMixin(BaseOutputPatternMixin):
    @staticmethod
    async def handle_output(output, target_qs):
        for target_q in target_qs:
            q_sizes = [t.qsize() for t in target_qs]
            min_q_idxs = [q for i, q in enumerate(q_sizes) if q == min(q_sizes)]
            target_q = target_qs[random.choice(min_q_idxs)]
            await target_q.put(output)


class BaseExecutorMixin:
    @contextmanager
    def pool(self, func_type):
        # defaults based on function type
        if func_type in (FuncType.async_method, FuncType.async_gen):
            pool_cls = None
        else:
            pool_cls = concurrent.futures.ThreadPoolExecutor

        # Defaults are overridden by user settings
        if self.parallelisation_method == ParallelisationMethod.event_loop:
            pool_cls = None
        elif self.parallelisation_method == ParallelisationMethod.process_pool:
            pool_cls = concurrent.futures.ProcessPoolExecutor
        elif self.parallelisation_method == ParallelisationMethod.thread_pool:
            pool_cls = concurrent.futures.ThreadPoolExecutor

        pool = None
        try:
            pool = pool_cls(max_workers=self.max_concurrency) if pool_cls else None
            yield pool
        # release resources
        finally:
            if pool:
                pool.shutdown(wait=True)

    async def run(self, exec_func, target_qs, input_q, inpt, kwargs, pool=None, context=None):
        """Coroutine for running exec func"""
        raise NotImplementedError('Implement me depending on execution function type.')


class AsyncGenExecutorMixin(BaseExecutorMixin):
    async def run(self, exec_func, target_qs, input_q, inpt, kwargs, pool=None, context=None):
        try:
            if context:
                kwargs['context'] = context
            if self.is_root:
                args = []  # don't pass input to root node (no input available)
            else:
                args = [inpt]
            async for output in exec_func(*args, **kwargs):
                await self.handle_output(output, target_qs)
        finally:
            if input_q is not None:
                input_q.task_done()


class SyncGenExecutorMixin(BaseExecutorMixin):
    async def run(self, exec_func, target_qs, input_q, inpt, kwargs, pool=None, context=None):
        try:
            if context:
                kwargs['context'] = context
            if self.is_root:
                # don't pass input to root node (no input available)
                blocking_generator = self.execute(**kwargs)
            else:
                blocking_generator = self.execute(inpt, **kwargs)

            def catch_stop_next(gen):
                try:
                    res = next(gen)
                    return res, False
                except StopIteration:
                    return None, True

            if pool is None:
                # run as blocking call in event loop
                while True:
                    output, gen_finished = catch_stop_next(blocking_generator)
                    if gen_finished:
                        break
                    await self.handle_output(output, target_qs)
            else:
                logging.info('Running func {0} with max_concurrency {1}'.format(self.task_id, self.max_concurrency))
                loop = asyncio.get_running_loop()
                while True:
                    output, gen_finished = await loop.run_in_executor(
                        pool, catch_stop_next, blocking_generator
                    )
                    if gen_finished:
                        break
                    await self.handle_output(output, target_qs)
        finally:
            if input_q is not None:
                input_q.task_done()


class AsyncMethodExecutorMixin(BaseExecutorMixin):
    async def run(self, exec_func, target_qs, input_q, inpt, kwargs, pool=None, context=None):
        try:
            if context:
                kwargs['context'] = context
            output = await exec_func(inpt, **kwargs)
            await self.handle_output(output, target_qs)
        finally:
            if input_q is not None:
                input_q.task_done()


class SyncMethodExecutorMixin(BaseExecutorMixin):
    async def run(self, exec_func, target_qs, input_q, inpt, kwargs, pool=None, context=None):
        try:
            if context:
                kwargs['context'] = context
            if pool:
                blocking_function = functools.partial(exec_func, inpt, **kwargs)
                loop = asyncio.get_running_loop()
                output = await loop.run_in_executor(pool, blocking_function)
            else:
                output = await exec_func(inpt, **kwargs)
            await self.handle_output(output, target_qs)
        finally:
            if input_q is not None:
                input_q.task_done()


class Node:
    """
    An extension of an Anytree Node. The tree is used as an easy way to contruct a DAG.
    This class builds a coroutine that can read data from it's input queue and submit data to it's target queue(s).
    After reading from the input queue it creates a new task to handle the data with the task logic that
    is passed in at construction/class definition.
    """
    def __new__(cls, *args, **kwargs):
        """Class factory to dynamically add mixins based on exec func type and output pattern."""

        # on deepcopy, this method is called on an already constructed class.
        if hasattr(cls, 'run') and hasattr(cls, 'handle_output'):
            return object.__new__(cls)

        cls_name = cls.__name__
        mixins = tuple()

        if kwargs.get('execute') or hasattr(cls, 'execute'):
            # If this is not true, async_worker_func is used
            exec_func = kwargs.get('execute') or getattr(cls, 'execute')
            exec_func_type = FuncType.classify(exec_func)
            output_pattern = kwargs.get('output_pattern')
            # OutputPattern Mixin
            if output_pattern in (None, OutputPattern.fanout):
                mixins  += (FanoutPatternMixin,)
                cls_name += 'Fanout'
            elif output_pattern == OutputPattern.load_balance:
                mixins  += (LoadBalancerPatternMixin,)
                cls_name += 'LoadBalanacing'
            else:
                raise ValueError("Unexpected OutputPattern %s." % output_pattern)

            # Executor Mixin
            if exec_func_type == FuncType.async_gen:
                mixins  += (AsyncGenExecutorMixin,)
                cls_name += 'AsyncGen'
            elif exec_func_type == FuncType.sync_gen:
                mixins  += (SyncGenExecutorMixin,)
                cls_name += 'SyncGen'
            elif exec_func_type == FuncType.async_method:
                mixins  += (AsyncMethodExecutorMixin,)
                cls_name += 'AyncMethod'
            elif exec_func_type == FuncType.sync_method:
                mixins  += (SyncMethodExecutorMixin,)
                cls_name += 'SyncMethod'
            else:
                raise ValueError("Unexpected execution function type %s." % exec_func_type)
        else:
            raise ValueError(f"Node {cls} should have an execute function.")

        bases = mixins + (anytree.NodeMixin, cls)
        attr_dct = {}
        mixed_cls = type(cls_name, bases, attr_dct)
        instance = object.__new__(mixed_cls)
        return instance

    def __init__(self, task_id, pipeline, setup=None, execute=None, teardown=None,
                 output_pattern=OutputPattern.fanout, parallelisation_method: ParallelisationMethod = None,
                 max_concurrency=None, kwargs=None):
        """
        Pass in the logic for your task as well as which output pattern to use for data propagation.

        :param task_id: Unique name (string) for the task
        :param pipeline: Pipeline object that the task belongs to
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
        self.task_id = task_id
        self.pipeline.nodes[task_id] = self
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
        self.context = {}

    @property
    def tasks(self):
        return self.pipeline.tasks[self.task_id]

    @property
    def async_worker_func(self):
            return self.async_worker_func_builder()

    def async_worker_func_builder(self):
        if not hasattr(self, 'execute'):
            raise ValueError("Please supply an execute function.")

        async def worker_func(
                input_q,
                target_qs,
                **kwargs
        ):

            if hasattr(self, 'setup'):
                func_type = FuncType.classify(self.setup)
                if func_type == FuncType.async_method:
                    context = await self.setup(**self.kwargs)
                    self.context.update(context)
                elif func_type == FuncType.sync_method:
                    # Note: this will block the event loop.
                    context = self.setup(**self.kwargs)
                    self.context.update(context)
                else:
                    raise ValueError("Unexpected function type for setup function. "
                                     "Has to be synchronous or async function or class method."
                                     "No generators allowed.")

            func_type = FuncType.classify(self.execute)
            with self.pool(func_type) as pool:
                try:
                    # First task generates its own data, so we don't need to await the previous stage
                    # since there is no previous stage.
                    if self.is_root:
                        task = asyncio.create_task(self.run(self.execute, target_qs, input_q, None, kwargs, pool=pool, context=self.context))
                        self.pipeline.tasks[self.task_id].append(task)
                    else:
                        # build exec runner based on type of exec func, and output pattern (policy based),
                        # pass in max concurrency
                        while True:
                            inpt = None
                            try:
                                inpt = input_q.get_nowait()
                            except asyncio.queues.QueueEmpty:
                                parents = []
                                if hasattr(self, 'parent') and self.parent is not None:
                                    if hasattr(self.parent, 'parent') and self.parent.parent is not None:
                                        parents = self.parent.parent.children
                                    else:
                                        parents = [self.parent]
                                finished_parents = [self.pipeline.workers[p.task_id].done() for p in parents]
                                started_parents = [len(self.tasks) > 0 for p in parents]

                                if all(finished_parents) and all(started_parents):
                                    parent_jobs_finished = [all(map(lambda x: x.done(), parent.tasks)) for parent in parents]
                                    if all(parent_jobs_finished) and input_q.empty():
                                        break
                                else:
                                    await asyncio.sleep(0.00001)

                            if inpt is not None:
                                task = asyncio.create_task(self.run(self.execute, target_qs, input_q, inpt, kwargs, pool=pool, context=self.context))
                                self.pipeline.tasks[self.task_id].append(task)

                                # limit concurrency if required
                                while self.max_concurrency and \
                                    len(list(filter(lambda t: t.done(), self.pipeline.tasks[self.task_id]))) >= self.max_concurrency:
                                    await asyncio.sleep(0.001)

                finally:
                    # allow all tasks generated by this node to finish before teardown
                    await asyncio.gather(*self.pipeline.tasks[self.task_id])
                    if hasattr(self, 'teardown'):
                        func_type = FuncType.classify(self.setup)
                        if inspect.iscoroutinefunction(self.teardown):
                            await self.teardown(self.context, **self.kwargs)
                        elif func_type == FuncType.sync_method:
                            # Note: this will block the event loop.
                            self.teardown(self.context, **self.kwargs)
                        else:
                            raise ValueError("Unexpected function type for teardown function. "
                                             "Has to be synchronous or async function or class method."
                                             "No generators allowed.")

        return worker_func

    def prime(self, input_q, target_qs):
        """Prime the associated coroutine"""
        if not self.coro:
            self.coro = self.async_worker_func(input_q, target_qs, **self.kwargs)
            task = asyncio.create_task(self.coro)
            self.pipeline.workers[self.task_id] = task
            if self.is_root:
                self.pipeline.root_worker = task

    def set_downstream(self, others):
        """
        Other(s) is downstream from self.
        """
        # Difference between a tree and a dag is that
        # the tree can't join up branches that have diverged
        # like a DAG can.
        # To add this DAG capability, we check if the task is
        # already used somewhere, and if so, we copy it, but
        # make sure it references the same coroutine.
        # Also ensure that the mutable data structures available
        # to the setup, exec and teardown functions are shared.
        if isinstance(others, (list, tuple, set)):
            others_copy = []
            for other in others:
                if other in self.root.descendants:  # other is already present in the graph
                    other_proxy = deepcopy(other)
                    other_proxy.kwargs = other.kwargs
                    other_proxy.context = other.context
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
            if other in self.root.descendants:  # other is already present in the graph
                other_proxy = deepcopy(other)
                other_proxy.kwargs = other.kwargs
                other_proxy.context = other.context
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


class FuncType:
    """
    Use introspeciton to classify which type of function it is.
    """
    sync_method = 'Synchronous function or class method'
    async_method = 'Async function or class method'
    sync_gen = 'Synchronous generator function or generator class method'
    async_gen = 'Asynchronous generator function or generator class method'

    @staticmethod
    def classify(func):
        # Treat execute func as an async generator
        if inspect.isasyncgenfunction(func):
            return FuncType.async_gen
        # Treat execute func as a normal generator
        elif inspect.isgeneratorfunction(func):
            return FuncType.sync_gen
        # Treat execute func as an async callable
        elif inspect.iscoroutinefunction(func):
            return FuncType.async_method
        # Treat execute func as a normal callable
        elif inspect.isfunction(func) or inspect.ismethod(func):
            return FuncType.sync_method
        else:
            raise ValueError("Unexpected function type for task.")
