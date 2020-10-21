import asyncio
from anytree import Node, RenderTree
from collections import defaultdict
from copy import deepcopy


class Pipeline:
    def __init__ (self):
        self.nodes = {}
        self.queues = defaultdict(dict)
        self.root_task = None
        self.root_node = None

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
                if node.is_root:
                    self.root_task = node.coro
                    self.root_node = node
                    break

    async def abuild(self, leaf_nodes=None, stop_before: str = None):
        """
        TODO: test with pipeline of unity length
        """
        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            q = asyncio.Queue()
            self.queues[leaf.coro_func.__qualname__]['input'] = q
            self.queues[leaf.coro_func.__qualname__]['targets'] = None
            leaf.coro = leaf.coro_func(self.queues, *leaf.args, **leaf.kwargs)
            asyncio.create_task(leaf.coro)
            node = leaf
            while True:
                parent = node.parent
                parent_targets = []
                children = parent.children
                for child in children:
                    if child.coro is None:
                        if child.is_leaf:
                            q = asyncio.Queue()
                            self.queues[child.coro_func.__qualname__]['input'] = q
                            self.queues[child.coro_func.__qualname__]['targets'] = None
                            child.coro = child.coro_func(self.queues, *child.args, **child.kwargs)
                            asyncio.create_task(child.coro)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                await self.build(leaf_nodes=[descendant_leaf], stop_before=parent.name)
                    parent_targets.append(child.coro)

                if parent.name == stop_before:
                    print("Build complete.")
                    return
                if not parent.coro:
                    q = asyncio.Queue()
                    self.queues[parent.coro_func.__qualname__]['input'] = q
                    self.queues[parent.coro_func.__qualname__]['targets'] = [self.queues[f.__qualname__]['input']
                                                                             for f in parent_targets]
                    parent.coro = parent.coro_func(self.queues, *parent.args, **parent.kwargs)
                    asyncio.create_task(parent.coro)

                node = parent
                if node.is_root:
                    self.root_task = node.coro
                    self.root_node = node
                    break

        print("Build complete.")

    async def run(self, async_generator):
        pipeline_root = self.root_task
        root_input_q = self.queues[pipeline_root.__qualname__]['input']
        async for url in async_generator:
            print(f"Sending url: {url}")
            await root_input_q.put(url)
        await self.join_all_input_queues()

    async def join_all_input_queues(self):
        input_queues = [d['input'] for d in self.queues.values()]
        for input_queue in input_queues:
            await input_queue.join()

    def render(self):
        for pre, fill, node in RenderTree(self.root_node):
            print("%s%s" % (pre, node.name))


class Task(Node):
    def __init__(self, name, pipeline, coro_func, args=None, kwargs=None):
        self.pipeline = pipeline
        if name in self.pipeline.nodes:
            raise ValueError(f'Task name must be unique, but `{name}` already exists.')
        self.pipeline.nodes[name] = self
        self.coro_func = coro_func
        self.coro = None
        self.args = args or []
        self.kwargs = kwargs or {}
        self.targets = None
        super().__init__(name)

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
                    other_copy = deepcopy(other)
                    other_copy.coro = other.coro
                    other = other_copy
                others_copy.append(other)
            others = others_copy

            if self.children:
                self.children += tuple(c for c in others)
            else:
                self.children = tuple(c for c in others)
        else:
            other = others
            if other in self.root.descendants:
                other_copy = deepcopy(other)
                other_copy.coro = other.coro
                other = other_copy

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

