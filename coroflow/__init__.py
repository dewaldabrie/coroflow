from anytree import Node, RenderTree
from copy import deepcopy


class Pipeline:
    def __init__ (self):
        self.nodes = {}
        self.root_task = None
        self.root_node = None

    def build(self, leaf_nodes=None, stop_before:str=None):
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

    async def abuild(self, leaf_nodes=None, stop_before:str=None):
        if not leaf_nodes:
            leaf_nodes = list(self.nodes.values())[0].root.leaves
        for leaf in leaf_nodes:
            leaf.coro = leaf.coro_func(None, *leaf.args, **leaf.kwargs)
            await leaf.coro.asend(None)
            node = leaf
            while True:
                parent = node.parent
                parent_targets = []
                children = parent.children
                for child in children:
                    if child.coro is None:
                        if child.is_leaf:
                            child.coro = child.coro_func(None, *child.args, **child.kwargs)
                            await child.coro.asend(None)
                        else:
                            descendant_leaves = [d for d in child.descendants if d.is_leaf]
                            for descendant_leaf in descendant_leaves:
                                await self.build(leaf_nodes=[descendant_leaf], stop_before=parent.name)
                    parent_targets.append(child.coro)

                if parent.name == stop_before:
                    return
                if not parent.coro:
                    parent.coro = parent.coro_func(parent_targets, *parent.args, **parent.kwargs)
                    await parent.coro.asend(None)

                node = parent
                if node.is_root:
                    self.root_task = node.coro
                    self.root_node = node
                    break

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

