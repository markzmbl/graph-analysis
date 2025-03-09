from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Sequence
from copy import deepcopy
from pprint import pprint

import networkx as nx
import numpy as np
from nbclient.client import timestamp
from networkx.classes.filters import show_nodes, no_filter

from dscent.types_ import Timestamp, TimeDelta, ReachabilitySet, SingleTimedVertex, Vertex, TimeSequence, \
    MultiTimedVertex, BundledPath


class TransactionGraph(nx.MultiDiGraph):
    def begin(self) -> Timestamp:
        *_, end_timestamp = next(iter(self.edges(keys=True)))
        return end_timestamp

    def length(self) -> TimeDelta:
        timestamps = [timestamp for *_, timestamp in self.edges(keys=True)]
        timestamps = sorted(timestamps)
        if not timestamps:
            return 0
        return timestamps[-1] - timestamps[0]

    @staticmethod
    def _begin_filter(begin: Timestamp, strict):
        def begin_filter(u, v, key):
            return begin < key if strict else begin <= key

        return begin_filter

    @staticmethod
    def _end_filter(end: Timestamp, strict):
        def end_filter(u, v, key):
            return key < end if strict else key <= end

        return end_filter

    def time_slice(
            self,
            begin: Timestamp | None = None,
            end: Timestamp | None = None,
            nodes: Sequence[Vertex] | None = None,
            closed: bool = False
    ):
        """
        Returns a subgraph view that filters edges based on the temporal constraints.
        Assumes edges have a timestamp as their origin.

        :param begin: Minimum timestamp (inclusive)
        :param end: Maximum timestamp (inclusive)
        :return: A subgraph view with filtered edges
        """
        begin_filter = no_filter
        if begin is not None:
            begin_filter = self._begin_filter(begin, strict=False)

        end_filter = no_filter
        if end is not None:
            end_filter = self._end_filter(end, strict=False if closed else True)

        def interval_filter(u, v, key):
            return begin_filter(u, v, key) and end_filter(u, v, key)

        return nx.subgraph_view(
            self,
            filter_node=show_nodes(nodes) if nodes is not None else no_filter,
            filter_edge=interval_filter
        )

    def prune(self, lower_time_limit: Timestamp):
        self.remove_edges_from(list(self.time_slice(end=lower_time_limit).edges(keys=True)))


class _ClosureManager(defaultdict[Vertex, Timestamp]):
    def __init__(self):
        super().__init__(lambda: np.inf)
        self.dependencies: defaultdict[Vertex, ReachabilitySet] = defaultdict(ReachabilitySet)

    def add_dependency(self, origin: Vertex, dependency: SingleTimedVertex):
        self.dependencies[origin].trim_after(dependency.timestamp)
        self.dependencies[origin].append(dependency)


class ExplorationGraph(TransactionGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closure = _ClosureManager()

    def cascade_closure(self, origin: Vertex, closing_time: Timestamp):
        dependencies = self.closure.dependencies[origin]
        idx = dependencies.get_trim_index(closing_time, strict=True, left=False)

        for dependency in dependencies[idx:]:
            timestamps = TimeSequence(self[dependency.vertex][origin])
            timestamps.trim_before(closing_time, strict=False)

            if len(timestamps) > 0:
                self.closure.add_dependency(
                    origin=origin,
                    dependency=SingleTimedVertex(vertex=dependency.vertex, timestamp=timestamps.begin())
                )
            timestamps.trim_after(closing_time)
            if len(timestamps) > 0 and timestamps.end() > self.closure[dependency.vertex]:
                self.closure[dependency.vertex] = timestamps.end()
                self.cascade_closure(origin=dependency.vertex, closing_time=timestamps.end())

        del dependencies[idx:]

    def activated_edges(self, current_timestamp: Timestamp, strict=False):
        begin_filter = self._begin_filter(begin=current_timestamp, strict=True)

        def activation_filter(u, v, key):
            closing_time = self.closure[v]
            return (
                    begin_filter(u, v, key)
                    and (key < closing_time if strict else key <= closing_time)
            )

        return nx.subgraph_view(self, filter_edge=activation_filter)

    def _simple_cycles(self, root_vertex: Vertex, path: BundledPath) -> Timestamp:
        *_, head = path

        current_vertex = head.vertex
        current_timestamp = head.begin()
        self.closure[current_vertex] = current_timestamp
        closing_time = 0

        # Adjacency View
        successor_view = self.activated_edges(current_timestamp)[current_vertex]

        if root_vertex in successor_view:  # Cycle detected
            timestamps = TimeSequence(successor_view[root_vertex])
            closing_time = max(timestamps.end(), closing_time)
            cycle = BundledPath(path)
            cycle.append(MultiTimedVertex(vertex=root_vertex, timestamps=timestamps))
            print(cycle)
            # yield cycle

        for successor_vertex, timestamps in successor_view.items():
            if successor_vertex == root_vertex:
                continue
            timestamps = TimeSequence(timestamps)
            timestamps.trim_after(upper_limit=self.closure[successor_vertex])
            if len(timestamps) == 0:
                continue

            next_path = BundledPath(path)
            next_path.append(MultiTimedVertex(vertex=successor_vertex, timestamps=timestamps))

            new_closing_time = self._simple_cycles(root_vertex, next_path)
            closing_time = max(new_closing_time, closing_time)

            timestamps = TimeSequence(successor_view[successor_vertex])
            timestamps.trim_before(new_closing_time)

            if len(timestamps) > 0:  # TODO: check
                self.closure.add_dependency(
                    origin=successor_vertex,
                    dependency=SingleTimedVertex(vertex=current_vertex, timestamp=timestamps.begin())
                )
        if closing_time > current_timestamp:
            self.closure[current_vertex] = closing_time
            self.cascade_closure(origin=current_vertex, closing_time=closing_time)

        return closing_time

    def simple_cycles(self, root_vertex: Vertex, next_candidates_begin_timestamp: Timestamp):
        for successor_vertex in self.successors(root_vertex):
            timestamps = TimeSequence(self[root_vertex][successor_vertex])
            timestamps.trim_after(next_candidates_begin_timestamp)
            # yield from self._simple_cycles(
            self._simple_cycles(
                root_vertex,
                BundledPath([MultiTimedVertex(vertex=successor_vertex, timestamps=timestamps)])
            )

