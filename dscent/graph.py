from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Sequence

import networkx as nx
import numpy as np
from networkx.classes.filters import show_nodes, no_filter

from dscent.types_ import Timestamp, TimeDelta, ReachabilitySet, TimedVertex, Vertex


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
        Assumes edges have a time as their key.

        :param begin: Minimum time (inclusive)
        :param end: Maximum time (inclusive)
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


class ExplorationGraph(TransactionGraph):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.closing_time = defaultdict(lambda: np.inf)
        self.unblock_list = defaultdict(ReachabilitySet)

    def activated_edges(self, current_timestamp: Timestamp, strict=False):
        begin_filter = self._begin_filter(begin=current_timestamp, strict=True)

        def activation_filter(u, v, key):
            closing_time = self.closing_time[v]
            return (
                begin_filter(u, v, key)
                and (key <= closing_time if strict else key < closing_time)
            )

        return nx.subgraph_view(self, filter_edge=activation_filter)

    def simple_cycles(self, root_vertex: Vertex, ):
        recursion_stack: list[tuple[Timestamp | None, ReachabilitySet]] = []

        for successor_vertex in self.successors(root_vertex):
            timestamp_view = self[root_vertex][successor_vertex]
            recursion_stack.append((
                0,  # lastp
                ReachabilitySet([TimedVertex(vertex=successor_vertex, time=timestamp_view)])
            ))

        while recursion_stack:
            lastp, path = recursion_stack.pop()
            *_, head = path
            assert isinstance(head.time, Iterable)
            assert sorted(head.time) == head.time

            current_vertex = head.vertex
            current_timestamp = head.begin()
            self.closing_time[current_vertex] = current_timestamp

            # Adjacency View
            successor_view = self.activated_edges(current_timestamp)[current_vertex]
            strict_successor_view = self.activated_edges(current_timestamp, strict=True)[current_vertex]

            if successor_view[root_vertex]:  # Cycle detected
                timestamps = list(successor_view[root_vertex])
                assert sorted(timestamps) == timestamps
                lastp = max(next(reversed(timestamps)), lastp)
                yield path + [TimedVertex(vertex=root_vertex, time=timestamps)]

            for successor_vertex in successor_view:
                if successor_vertex == root_vertex:
                    continue

                timestamps = list(strict_successor_view[current_vertex][successor_vertex])
                assert sorted(timestamps) == timestamps

                if len(timestamps) == 0:
                    continue

                recursion_stack.append((  # type: ignore
                    lastp,
                    path + [TimedVertex(vertex=current_vertex, time=timestamps)]  # type: ignore
                ))
                minimum_timestamp = timestamps[bisect_right(timestamps, lastp)]
                self.unblock_list[successor_vertex].append(Timestamp(vertex=current_vertex, time=minimum_timestamp))

            self.unblock(current_vertex, lastp)
