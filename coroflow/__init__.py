import asyncio
from anytree import Node, RenderTree, PreOrderIter
from collections import defaultdict
from copy import deepcopy
from pprint import pprint


class Pipeline:
    def __init__ (self):
        self.nodes = {}
        self.queues = defaultdict(dict)
        self.root_task = None
        self.root_node = None
        self.proxy_nodes_by_task_id = {}

    def build(self, leaf_nodes=None, stop_before: str = None):
        """
        TODO: test with pipeline of unity length
        """
        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            leaf.coro = leaf.coro_func(None, *leaf.args, **leaf.kwargs)
            leaf.coro.send(None)
            node = leaf
            while True:
                if node.is_root:
                    self.root_task = node.coro
                    self.root_node = node
                    self.root_task.send(None)
                    break
                parent = node.parent
                parent_targets = []
                children = parent.children
                for child in children:
                    if child.coro is None:
                        if child.is_leaf:
                            child.coro = child.coro_func(None, *child.args, **child.kwargs)
                            child.coro.send(None)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                self.build(leaf_nodes=[descendant_leaf], stop_before=parent.name)
                    parent_targets.append(child.coro)

                if parent.name == stop_before:
                    return
                if not parent.coro:
                    parent.coro = parent.coro_func(parent_targets, *parent.args, **parent.kwargs)
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
            q = asyncio.Queue()
            self.queues[node.task_id]['input'] = q
            self.queues[node.task_id]['targets'] = targets
            node.prime(self.queues)
        else:
            # make sure the coro is shared between proxy and original
            if node.task_id not in self.queues:
                q = asyncio.Queue()
                self.queues[node.task_id]['input'] = q
                self.queues[node.task_id]['targets'] = targets
                node.is_proxy_for_node.prime(self.queues)
            node.coro = node.is_proxy_for_node.coro

    async def abuild(self, leaf_nodes=None, stop_before: str = None):
        """
        TODO: test with pipeline of unity length
        """

        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            self.coro_setup(leaf)
            node = leaf
            while True:
                if node.is_root:
                    node.prime(self.queues)
                    self.root_task = node.coro
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
                    print("Build complete.")
                    return
                if not parent.coro:
                    self.coro_setup(parent, targets=[self.queues[task_id]['input'] for task_id in parent_targets])

                node = parent

        print("Build complete.")

    async def run(self, async_generator):
        root_input_q = self.queues[self.root_node.task_id]['input']
        async for url in async_generator:
            await root_input_q.put(url)
        await self.join_all_input_queues()

    async def join_all_input_queues(self):
        for node in PreOrderIter(self.root_node):
            task_id = node.task_id
            input_queue = self.queues[task_id]['input']
            pprint(f"Joining queue for {task_id}")
            await input_queue.join()
            pprint(f"Done joining queue for {task_id}")

    def render(self):
        for pre, fill, node in RenderTree(self.root_node):
            print("%s%s" % (pre, node.name))


class Task(Node):
    def __init__(self, task_id, pipeline, coro_func, args=None, kwargs=None):
        self.pipeline = pipeline
        if task_id in self.pipeline.nodes:
            raise ValueError(f'Task task_id must be unique, but `{task_id}` already exists.')
        self.pipeline.nodes[task_id] = self
        self.coro_func = coro_func
        self.coro = None
        self.args = args or []
        self.kwargs = kwargs or {}
        self.targets = None
        self.is_proxy_for_node = None
        super().__init__(task_id)

    def prime(self, queues):
        """Prime the associated coroutine"""
        if not self.coro:
            self.coro = self.coro_func(queues, *self.args, **self.kwargs, task_id=self.task_id)
            asyncio.create_task(self.coro)


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

