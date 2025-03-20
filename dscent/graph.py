from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from copy import deepcopy
from typing import DefaultDict

import networkx as nx
import numpy as np
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


def _get_infinity():
    return np.inf


class _ClosureManager(DefaultDict[Vertex, Timestamp]):
    def __init__(self):
        super().__init__(_get_infinity)
        self.dependencies: DefaultDict[Vertex, ReachabilitySet] = defaultdict(ReachabilitySet)

    def add_dependency(self, origin: Vertex, dependency: SingleTimedVertex):
        # U(v) ← U(v) \ {(w, t′)}
        self.dependencies[origin].trim_after(dependency.timestamp)
        # U(v) ← U(v) ∪ {(w, t)}
        self.dependencies[origin].append(dependency)


class ExplorationGraph(TransactionGraph):
    def __init__(self, *args, root_vertex: Vertex | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph["root_vertex"] = root_vertex
        self.closure = _ClosureManager()

    def __getstate__(self):
        state = self.__dict__.copy()
        # Convert closure to a serializable format
        state["closure"] = dict(self.closure)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # Restore closure as a _ClosureManager instance
        self.closure = _ClosureManager()
        self.closure.update(state["closure"])

    @property
    def root_vertex(self) -> Vertex:
        return self.graph["root_vertex"]

    def cascade_closure(self, origin: Vertex, closing_time: Timestamp):
        # for (w, t_w) ∈ U(v) do
        dependencies = self.closure.dependencies[origin]
        # if t_w < t_v then
        idx = dependencies.get_trim_index(closing_time, strict=True, left=False)

        for dependency in dependencies[idx:]:
            # T[w, v] = {t | (w, v, t) ∈ E}
            # T ← {t ∈ T[w, v] | t_v ≤ t}
            timestamps = TimeSequence(self[dependency.vertex][origin])
            timestamps.trim_before(closing_time, strict=False)

            # if T ≠ ∅ then
            if len(timestamps) > 0:
                # U(v) ← U(v) ∪ {(w, min(T))}
                self.closure.add_dependency(
                    origin=origin,
                    dependency=SingleTimedVertex(vertex=dependency.vertex, timestamp=timestamps.begin())
                )
            # t_max ← max {t ∈ T[w, v] | t < t_v}
            timestamps.trim_after(closing_time)
            if len(timestamps) > 0 and timestamps.end() > self.closure[dependency.vertex]:
                self.closure[dependency.vertex] = timestamps.end()
                # Unblock(w, t_max)
                self.cascade_closure(origin=dependency.vertex, closing_time=timestamps.end())
        # U(v) ← U(v) \ {(w, tw)}
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

    def _simple_cycles(self, path: BundledPath, cycles: list[BundledPath]) -> tuple[Timestamp, list[BundledPath]]:
        *_, head = path
        # v_cur ← v_k
        current_vertex = head.vertex
        # t_cur ← min(Tk)
        current_timestamp = head.begin()
        # Set closure time explicitly
        # ct(v_cur) ← t_cur
        self.closure[current_vertex] = current_timestamp
        # lastp ← 0
        closing_time = 0

        # Adjacency View
        # Out ← {(v_cur, x, t) ∈ E | t_cur < t ≤ ct(x)}
        # N ← {x ∈ V |(v_cur, x, t) ∈ Out}
        successor_view = self.activated_edges(current_timestamp)[current_vertex]

        # Determine whether a cycle is present
        # if s ∈ N then

        cycle_time_sequence = TimeSequence()
        if (
                self.root_vertex in successor_view
                # T ← {t | (v_cur, s, t) ∈ Out}
                and (len(cycle_time_sequence := TimeSequence(successor_view[self.root_vertex])) > 0)
        ):
            # If necessary update current closing_time to the latest cycle closing time
            # t ← max(T)
            # if t > lastp then
            #   lastp ← t
            closing_time = max(cycle_time_sequence.end(), closing_time)
            # Build new cycle
            cycle = BundledPath(path)
            # Expand(B, v_cur → Ts)
            try:
                cycle.append(MultiTimedVertex(vertex=self.root_vertex, timestamps=cycle_time_sequence))
                # Memorize new cycle
                cycles.append(cycle)
            except ValueError:
                pass

        # Iterate other succeeding vertices
        # for x ∈ N \ {s} do
        for successor_vertex, successor_timestamps in successor_view.items():
            if successor_vertex == self.root_vertex:
                continue
            # T_x ← {t | (v_cur, x, t) ∈ Out}
            successor_time_sequence = TimeSequence(successor_timestamps)
            # T_x′ ← {t ∈ Tx |t < ct(x)}
            successor_time_sequence.trim_after(upper_limit=self.closure[successor_vertex])
            # if T_x′ ≠ ∅ then
            if len(successor_time_sequence) == 0:
                continue

            # lastx ← AllBundles(s, Expand(B, v_cur -T_x'→ x)
            next_path = deepcopy(path)
            try:
                next_path.append(MultiTimedVertex(vertex=successor_vertex, timestamps=successor_time_sequence))
                new_closing_time, cycles = self._simple_cycles(next_path, cycles)
                # if last_x > lastp then
                #   lastp ← last_x
                closing_time = max(new_closing_time, closing_time)

                # t_m ← min({t ∈ Tx | t > last_x})
                open_time_sequence = TimeSequence(successor_timestamps)
                open_time_sequence.trim_before(new_closing_time)
                if len(open_time_sequence) > 0:  # TODO: check
                    # Extend(U(x), (v_cur, t_m))
                    self.closure.add_dependency(
                        origin=successor_vertex,
                        dependency=SingleTimedVertex(vertex=current_vertex, timestamp=open_time_sequence.begin())
                    )
            except ValueError:  # Timestamps incompatible
                pass

        if closing_time > current_timestamp:
            # Unblock(v_cur, lastp)
            self.closure[current_vertex] = closing_time
            self.cascade_closure(origin=current_vertex, closing_time=closing_time)

        return closing_time, cycles

    def simple_cycles(self, next_candidates_begin_timestamp: Timestamp) -> list[BundledPath]:
        all_cycles = []
        for successor_vertex in self.successors(self.root_vertex):
            timestamps = TimeSequence(self[self.root_vertex][successor_vertex])
            timestamps.trim_after(next_candidates_begin_timestamp)
            if len(timestamps) == 0:
                continue

            _, cycles = self._simple_cycles(
                path=BundledPath([MultiTimedVertex(vertex=successor_vertex, timestamps=timestamps)]),
                cycles=[]
            )
            all_cycles.extend(cycles)
        return all_cycles
