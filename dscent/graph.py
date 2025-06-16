from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from copy import deepcopy
from typing import DefaultDict, NamedTuple, Hashable

import networkx as nx
from networkx.classes.coreviews import AtlasView
from networkx.classes.filters import show_nodes, no_filter

from dscent.types_ import (
    Timestamp,
    PointVertex,
    Vertex,
    SeriesVertex, get_timestamp_from_attributes, )
from dscent.time_sequence import get_sequence_after, get_sequence_before
from dscent.reachability import DirectReachability, SequentialReachability


class TransactionKey(NamedTuple):
    timestamp: Timestamp
    edge_key: Hashable


class TemporalAtlas(dict):
    def timestamps(self):
        return tuple(key.timestamp for key in self.keys())


class TemporalAtlasView(AtlasView):
    def timestamps(self):
        return tuple(key.timestamp for key in self.keys())

    def __getitem__(self, key):
        return TemporalAtlas(super().__getitem__(key))


class TransactionGraph(nx.MultiDiGraph):
    def __getitem__(self, n):
        return TemporalAtlasView(super().__getitem__(n))

    def begin(self) -> Timestamp:
        *_, transaction_key = next(iter(self.edges(keys=True)))
        return transaction_key.timestamp

    def add_edge(self, u_for_edge, v_for_edge, key=None, **attr):
        if not isinstance(key, TransactionKey):
            key = TransactionKey(
                timestamp=get_timestamp_from_attributes(attr),
                edge_key=self.new_edge_key(u_for_edge, v_for_edge) if key is None else key
            )
        return super().add_edge(u_for_edge, v_for_edge, key=key, **attr)

    @staticmethod
    def _begin_filter(begin: Timestamp, include_limit):
        def begin_filter(u, v, key):
            ts = key.timestamp
            return begin < ts if include_limit else begin <= ts

        return begin_filter

    @staticmethod
    def _end_filter(end: Timestamp, include_limit):
        def end_filter(u, v, key):
            ts = key.timestamp
            return ts < end if include_limit else ts <= end

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
            begin_filter = self._begin_filter(begin, include_limit=False)

        end_filter = no_filter
        if end is not None:
            end_filter = self._end_filter(end, include_limit=False if closed else True)

        def interval_filter(u, v, key):
            return begin_filter(u, v, key) and end_filter(u, v, key)

        return nx.subgraph_view(
            self,
            filter_node=show_nodes(nodes) if nodes is not None else no_filter,
            filter_edge=interval_filter
        )

    def prune(self, lower_time_limit: Timestamp):
        self.remove_edges_from(list(self.time_slice(end=lower_time_limit).edges(keys=True)))
        self.remove_nodes_from(list(nx.isolates(self)))

    @classmethod
    def from_networkx(cls, graph: nx.MultiDiGraph) -> TransactionGraph:
        g = cls()
        for u, v, key, data in graph.edges(keys=True, data=True):
            g.add_edge(u, v, key=key, **data)
        return g

    def to_networkx(self):
        serializable_graph = nx.MultiDiGraph()
        serializable_graph.add_edges_from(
            (u, v, key.timestamp, data)
            for u, v, key, data
            in self.edges(keys=True, data=True)
        )
        return serializable_graph


class _ClosureManager(DefaultDict[Vertex, Timestamp]):
    @staticmethod
    def _infinity():
        return float("inf")

    def __init__(self, default_factory=None):  # Accept factory argument
        super().__init__(self._infinity)  # Ensure valid factory
        self.dependencies: DefaultDict[Vertex, DirectReachability] = defaultdict(DirectReachability)

    def add_dependency(self, origin: Vertex, dependency: PointVertex):
        # U(v) ← U(v) \ {(w, t′)}
        self.dependencies[origin].after(dependency.timestamp, inplace=True)
        # U(v) ← U(v) ∪ {(w, t)}
        self.dependencies[origin].add(dependency)


class ExplorationGraph(TransactionGraph):
    _closure: _ClosureManager

    def __init__(self, *args, root_vertex: Vertex | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._root_vertex = root_vertex
        self._closure = _ClosureManager()

    @property
    def root_vertex(self) -> Vertex:
        return self._root_vertex

    def cascade_closure(self, origin: Vertex, closing_time: Timestamp):
        # for (w, t_w) ∈ U(v) do
        dependencies = self._closure.dependencies[origin]
        # if t_w < t_v then
        # U(v) ← U(v) \ {(w, tw)}
        dependencies.before(closing_time, include_limit=False, inplace=True)

        for ts in dependencies.timestamps:
            # T[w, v] = {t | (w, v, t) ∈ E}
            # T ← {t ∈ T[w, v] | t_v ≤ t}
            for root in dependencies[ts]:
                timestamps = get_sequence_after(self[root][origin].timestamps(), limit=closing_time, include_limit=True)

                # if T ≠ ∅ then
                if len(timestamps) > 0:
                    # U(v) ← U(v) ∪ {(w, min(T))}
                    self._closure.add_dependency(
                        origin=origin,
                        dependency=PointVertex(vertex=root, timestamp=timestamps[0])
                    )
                # t_max ← max {t ∈ T[w, v] | t < t_v}
                timestamps = get_sequence_before(timestamps, limit=closing_time, include_limit=False)
                if len(timestamps) > 0 and timestamps[-1] > self._closure[root]:
                    self._closure[root] = timestamps[-1]
                    # Unblock(w, t_max)
                    self.cascade_closure(origin=root, closing_time=timestamps[-1])

    def activated_edges(self, current_timestamp: Timestamp, strict=False):
        begin_filter = self._begin_filter(begin=current_timestamp, include_limit=False)

        def activation_filter(u, v, key):
            ts = key.timestamp
            closing_time = self._closure[v]
            return (
                    begin_filter(u, v, key)
                    and (ts < closing_time if strict else ts <= closing_time)
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

        if self.root_vertex in successor_view:
            # T ← {t | (v_cur, s, t) ∈ Out}
            cycle_time_sequence = successor_view[self.root_vertex].timestamps()
            if len(cycle_time_sequence) > 0:
                # If necessary update current closing_time to the latest cycle closing time
                # t ← max(T)
                # if t > lastp then
                #   lastp ← t
                closing_time = max(cycle_time_sequence[-1], closing_time)
                # Build new cycle
                cycle = deepcopy(path)
                # Expand(B, v_cur → Ts)
                try:
                    cycle.expand(SeriesVertex(vertex=self.root_vertex, timestamps=cycle_time_sequence))
                    # Memorize new cycle
                    cycles.append(cycle)
                except ValueError:
                    pass

        # Iterate other succeeding vertices
        # for x ∈ N \ {s} do
        for successor_vertex, successor_edges in successor_view.items():
            if successor_vertex == self.root_vertex:
                continue
            # T_x ← {t | (v_cur, x, t) ∈ Out}
            successor_time_sequence = successor_edges.timestamps()
            # T_x′ ← {t ∈ Tx |t < ct(x)}
            open_time_sequence = get_sequence_before(
                successor_time_sequence, limit=self._closure[successor_vertex], include_limit=False)
            # if T_x′ ≠ ∅ then
            if len(open_time_sequence) == 0:
                continue

            # lastx ← AllBundles(s, Expand(B, v_cur -T_x'→ x)
            next_path = deepcopy(path)
            try:
                next_path.expand(SeriesVertex(
                    vertex=successor_vertex,
                    timestamps=open_time_sequence)
                )
                new_closing_time, cycles = self._simple_cycles(next_path, cycles)
                # if last_x > lastp then
                #   lastp ← last_x
                closing_time = max(new_closing_time, closing_time)

                # t_m ← min({t ∈ T_x | t > last_x})
                closed_time_sequence = get_sequence_after(
                    successor_time_sequence,
                    limit=new_closing_time, include_limit=False
                )
                if len(closed_time_sequence) > 0:
                    # Extend(U(x), (v_cur, t_m))
                    self._closure.add_dependency(
                        origin=successor_vertex,
                        dependency=PointVertex(vertex=current_vertex, timestamp=closed_time_sequence[0])
                    )
            except ValueError:  # Timestamps incompatible
                pass

        if closing_time > current_timestamp:
            # Unblock(v_cur, lastp)
            self._closure[current_vertex] = closing_time
            self.cascade_closure(origin=current_vertex, closing_time=closing_time)

        return closing_time, cycles

    def simple_cycles(self, next_seed_begin: Timestamp) -> list[TransactionGraph]:
        all_cycles = []
        for successor_vertex in self.successors(self.root_vertex):
            timestamps = get_sequence_before(
                self[self.root_vertex][successor_vertex].timestamps(),
                next_seed_begin, include_limit=False
            )
            if len(timestamps) == 0:
                continue
            path = SequentialReachability(SeriesVertex(vertex=successor_vertex, timestamps=tuple(timestamps)))
            # Create a new path with the successor vertex and its timestamps
            _, sequential_reachabilities = self._simple_cycles(path=path, cycles=[])
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
            u, v = predecessor.vertex, successor.vertex
            for key, data in self[u][v].items():
                data = dict(data)
                data["timestamp"] = key.timestamp
                edges.append((u, v, key.edge_key, data))
        bundled_cycle = nx.MultiDiGraph(edges)
        return bundled_cycle

    @classmethod
    def from_networkx(cls, graph: nx.MultiDiGraph, root_vertex: Vertex) -> TransactionGraph:
        g = cls(root_vertex=root_vertex)
        for u, v, key, data in graph.edges(keys=True, data=True):
            g.add_edge(u, v, key=key, **data)
        return g
