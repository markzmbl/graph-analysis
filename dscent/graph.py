from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence, Iterable
from copy import deepcopy
from typing import DefaultDict

import networkx as nx
from networkx.classes.filters import show_nodes, no_filter

from dscent.types_ import (
    Timestamp,
    Timedelta,
    PointVertex,
    Vertex,
    FrozenTimeSequence,
    SeriesVertex, MutableTimeSequence,
)
from dscent.reachability import DirectReachability, SequentialReachability


class TransactionGraph(nx.MultiDiGraph):
    def begin(self) -> Timestamp:
        *_, end_timestamp = next(iter(self.edges(keys=True)))
        return end_timestamp

    def length(self) -> Timedelta:
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
            nodes: Iterable[Vertex] | None = None,
            closed: bool = False
    ):
        """
        Returns a subgraph view that filters edges based on temporal constraints.
        Assumes edges have a timestamp as their key.

        :param begin: The minimum timestamp for filtering edges. Edges with timestamps before this value are excluded.
        :param end: The maximum timestamp for filtering edges. Edges with timestamps after this value are excluded.
        :param nodes: A sequence of nodes to include in the subgraph. If None, all nodes are considered.
        :param closed:
            If True, the data_interval is closed [begin, upper_limit],
            meaning edges with timestamps exactly equal to `upper_limit` are included.
            If False, the data_interval is half-open [begin, upper_limit),
            meaning edges with `upper_limit` timestamp are excluded.
        :return: A subgraph view containing only edges within the specified time range and optionally filtered by nodes.
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


class _ClosureManager(DefaultDict[Vertex, Timestamp]):
    @staticmethod
    def _infinity():
        return float("inf")

    def __init__(self, default_factory=None):  # Accept factory argument
        super().__init__(self._infinity)  # Ensure valid factory
        self.dependencies: DefaultDict[Vertex, DirectReachability] = defaultdict(DirectReachability)

    def add_dependency(self, origin: Vertex, dependency: PointVertex):
        # U(v) ← U(v) \ {(w, t′)}
        self.dependencies[origin].trim_after(dependency.timestamp)
        # U(v) ← U(v) ∪ {(w, t)}
        self.dependencies[origin].append(dependency)


class BundledCycle(nx.MultiDiGraph):
    pass


class ExplorationGraph(TransactionGraph):
    _closure: _ClosureManager

    def __init__(self, *args, root_vertex: Vertex | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.graph["root_vertex"] = root_vertex
        self._closure = _ClosureManager()

    @property
    def root_vertex(self) -> Vertex:
        return self.graph["root_vertex"]

    def cascade_closure(self, origin: Vertex, closing_time: Timestamp):
        # for (w, t_w) ∈ U(v) do
        dependencies = self._closure.dependencies[origin]
        # if t_w < t_v then
        idx = dependencies.timestamps.get_split_index(closing_time, strict=True, left=False)

        for dependency in dependencies[idx:]:
            # T[w, v] = {t | (w, v, t) ∈ E}
            # T ← {t ∈ T[w, v] | t_v ≤ t}
            timestamps = FrozenTimeSequence(self[dependency.root][origin])
            timestamps.trim_before(closing_time, strict=False)

            # if T ≠ ∅ then
            if len(timestamps) > 0:
                # U(v) ← U(v) ∪ {(w, min(T))}
                self._closure.add_dependency(
                    origin=origin,
                    dependency=PointVertex(vertex=dependency.root, timestamp=timestamps.begin())
                )
            # t_max ← max {t ∈ T[w, v] | t < t_v}
            timestamps.trim_after(closing_time)
            if len(timestamps) > 0 and timestamps.end() > self._closure[dependency.root]:
                self._closure[dependency.root] = timestamps.end()
                # Unblock(w, t_max)
                self.cascade_closure(origin=dependency.root, closing_time=timestamps.end())
        # U(v) ← U(v) \ {(w, tw)}
        del dependencies[idx:]

    def activated_edges(self, current_timestamp: Timestamp, strict=False):
        begin_filter = self._begin_filter(begin=current_timestamp, strict=True)

        def activation_filter(u, v, key):
            closing_time = self._closure[v]
            return (
                    begin_filter(u, v, key)
                    and (key < closing_time if strict else key <= closing_time)
            )

        return nx.subgraph_view(self, filter_edge=activation_filter)

    def _simple_cycles(
            self, path: SequentialReachability, cycles: list[SequentialReachability]
    ) -> tuple[Timestamp, list[SequentialReachability]]:
        *_, head = path
        # v_cur ← v_k
        current_vertex = head.vertex
        # t_cur ← min(Tk)
        current_timestamp = head.begin()
        # Set _closure time explicitly
        # ct(v_cur) ← t_cur
        self._closure[current_vertex] = current_timestamp
        # lastp ← 0
        closing_time = 0

        # Adjacency View
        # Out ← {(v_cur, x, t) ∈ E | t_cur < t ≤ ct(x)}
        # N ← {x ∈ V |(v_cur, x, t) ∈ Out}
        successor_view = self.activated_edges(current_timestamp)[current_vertex]

        # Determine whether a cycle is present
        # if s ∈ N then

        if (
                self.root_vertex in successor_view
                # T ← {t | (v_cur, s, t) ∈ Out}
                and (len(cycle_time_sequence := FrozenTimeSequence(successor_view[self.root_vertex])) > 0)
        ):
            # If necessary update current closing_time to the latest cycle closing time
            # t ← max(T)
            # if t > lastp then
            #   lastp ← t
            closing_time = max(cycle_time_sequence.end(), closing_time)
            # Build new cycle
            cycle = SequentialReachability(path)
            # Expand(B, v_cur → Ts)
            try:
                cycle.append(SeriesVertex(vertex=self.root_vertex, timestamps=cycle_time_sequence))
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
            successor_time_sequence = MutableTimeSequence(successor_timestamps)
            # T_x′ ← {t ∈ Tx |t < ct(x)}
            successor_time_sequence.trim_after(upper_limit=self._closure[successor_vertex])
            # if T_x′ ≠ ∅ then
            if len(successor_time_sequence) == 0:
                continue

            # lastx ← AllBundles(s, Expand(B, v_cur -T_x'→ x)
            next_path = deepcopy(path)
            try:
                next_path.append(SeriesVertex(
                    vertex=successor_vertex,
                    timestamps=FrozenTimeSequence(successor_time_sequence))
                )
                new_closing_time, cycles = self._simple_cycles(next_path, cycles)
                # if last_x > lastp then
                #   lastp ← last_x
                closing_time = max(new_closing_time, closing_time)

                # t_m ← min({t ∈ Tx | t > last_x})
                open_time_sequence = MutableTimeSequence(successor_timestamps)
                open_time_sequence.trim_before(new_closing_time)
                if len(open_time_sequence) > 0:  # TODO: check
                    # Extend(U(x), (v_cur, t_m))
                    self._closure.add_dependency(
                        origin=successor_vertex,
                        dependency=PointVertex(vertex=current_vertex, timestamp=open_time_sequence.begin())
                    )
            except ValueError:  # Timestamps incompatible
                pass

        if closing_time > current_timestamp:
            # Unblock(v_cur, lastp)
            self._closure[current_vertex] = closing_time
            self.cascade_closure(origin=current_vertex, closing_time=closing_time)

        return closing_time, cycles

    def simple_cycles(self, next_seed_begin: Timestamp) -> list[BundledCycle]:
        all_cycles = []
        for successor_vertex in self.successors(self.root_vertex):
            timestamps = MutableTimeSequence(self[self.root_vertex][successor_vertex])
            timestamps.trim_after(next_seed_begin)
            if len(timestamps) == 0:
                continue
            _, sequential_reachabilities = self._simple_cycles(
                path=SequentialReachability([SeriesVertex(
                    vertex=successor_vertex,
                    timestamps=FrozenTimeSequence(timestamps)
                )]),
                cycles=[]
            )
            cycles = [
                self.to_bundled_cycle(sequential_reachability)
                for sequential_reachability in sequential_reachabilities
            ]
            all_cycles.extend(cycles)
        return all_cycles

    def to_bundled_cycle(self, sequential_reachability: SequentialReachability):
        edges = []
        assert len(sequential_reachability) > 0
        root, head = sequential_reachability[0], sequential_reachability[-1]
        cycle_edge_pairs = [(head, root)]
        cycle_edge_pairs += list(sequential_reachability.reverse_pairs())
        for predecessor, successor in cycle_edge_pairs:
            timestamps = successor.timestamps
            assert len(timestamps) > 0
            for timestamp in timestamps:
                u, v = predecessor.vertex, successor.vertex
                data = self.get_edge_data(u, v, timestamp, default={})
                edges.append((u, v, timestamp, data))
        return BundledCycle(edges)
