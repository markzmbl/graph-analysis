from __future__ import annotations

from bisect import bisect_right, bisect_left
from collections import defaultdict, Counter
from typing import NamedTuple
from collections.abc import Iterator, Hashable

import dynetx as dn
import networkx as nx
import numpy as np
from dynetx.algorithms import time_respecting_paths
from intervaltree import IntervalTree, Interval
from networkx.algorithms.simple_paths import all_simple_paths
from networkx.classes import DiGraph, MultiDiGraph

Vertex = Hashable
TimeStamp = int
TimeDelta = int

class TransactionGraph(nx.MultiDiGraph):
    def time_slice(self, begin: TimeStamp | None = None, end: TimeStamp | None = None):
        """
        Returns a subgraph view that filters edges based on the temporal constraints.
        Assumes edges have a timestamp as their key.

        :param begin: Minimum timestamp (inclusive)
        :param end: Maximum timestamp (exclusive)
        :return: A subgraph view with filtered edges
        """

        def edge_filter(u, v, key):
            return (begin is None or key >= begin) and (end is None or key < end)

        return nx.subgraph_view(self, filter_edge=edge_filter)

    def prune(self, lower_time_limit: TimeStamp):
        self.remove_edges_from(list(self.time_slice(end=lower_time_limit).edges(keys=True)))

class Interaction(NamedTuple):
    source: Vertex
    target: Vertex
    timestamp: TimeStamp


class ReverseReachableVertex(NamedTuple):
    vertex: Vertex
    timestamp: TimeStamp

    def __lt__(self, other: ReverseReachableVertex) -> bool:
        return (self.timestamp, self.vertex) < (other.timestamp, other.vertex)


class ReverseReachabilitySet(list[ReverseReachableVertex]):
    def prune(self, lower_time_limit: TimeStamp, strictly_smaller=False) -> None:
        """
        Return the pruned reverse reachability set where stale entries are removed.
        Assumes reverse_reachability_set is sorted by time_x.
        Uses binary search (`bisect_left`) for efficient pruning.
        """
        # assert sorted(set(self)) == self

        bisect_function = bisect_right if strictly_smaller else bisect_left

        # Use bisect with key parameter to find the first valid index
        index = bisect_function(self, lower_time_limit, key=lambda item: item.timestamp)

        # Update the list in place
        for i, (v, t) in enumerate(self):
            outside = t <= lower_time_limit if strictly_smaller else t < lower_time_limit
            assert outside and i < index or not outside and i >= index

        del self[:index]

    def __ior__(self, other: ReverseReachabilitySet) -> ReverseReachabilitySet:
        """Merge two sorted lists of unique tuples while preserving order and uniqueness."""
        i, j = 0, 0
        merged = ReverseReachabilitySet()

        while i < len(self) and j < len(other):
            if self[i] < other[j]:
                merged.append(self[i])
                i += 1
            elif other[j] < self[i]:
                merged.append(other[j])
                j += 1
            else:
                # They are equal, add only one copy
                merged.append(self[i])
                i += 1
                j += 1

        # Append remaining elements (if any)
        merged.extend(self[i:])
        merged.extend(other[j:])

        # assert sorted(set(self + other)) == merged

        return merged


class Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: TimeStamp | None = None  # Start time of the next interval, if available


class Seed(NamedTuple):
    vertex: Vertex
    begin: TimeStamp
    end: TimeStamp
    candidates: Candidates


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    vertex_seed_sizes_history: list | None = None
    interval_durations_history: list | None = None

    def __init__(
            self,
            interactions: Iterator[Interaction],
            omega: TimeDelta = 10,
            prune_interval: int = 1_000,
            combine_seeds: bool = True,
            track_history: bool = False
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self.interactions = iter(interactions)  # Stream of edges
        self.iteration_count: int = 0  # Track number of processed edges
        self.prune_interval: int = prune_interval  # Interval for pruning old data
        self.omega: TimeDelta = omega  # Maximum time window for relevant edges

        # Reverse reachability mapping: vertex -> sorted set of (lower_time_limit, predecessor) pairs
        self.reverse_reachability: dict[Vertex, ReverseReachabilitySet] = defaultdict(ReverseReachabilitySet)

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self.seeds: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)

        # Dynamic directed graph with edge removal enabled
        self.transaction_graph = TransactionGraph()

        self.combine_seeds = combine_seeds

        self.track_history = track_history
        if track_history:
            self.vertex_seed_sizes_history = []
            self.interval_durations_history = []

    def _update_reverse_reachability(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        # (a, b, t) ∈ E
        source, target, current_timestamp = (
            interaction.source,
            interaction.target,
            interaction.timestamp,
        )
        target_reverse_reachability = self.reverse_reachability[target]
        lower_time_limit = current_timestamp - self.omega

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(ReverseReachableVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.prune(lower_time_limit)

        if source not in self.reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self.reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.prune(lower_time_limit)

        # Propagate reachability
        # S(b) ← S(b) ∪ S(a)
        target_reverse_reachability |= source_reverse_reachability

        # for (b, tb) ∈ S(b) do
        cyclic_reachability = ReverseReachabilitySet(
            v for v in target_reverse_reachability if v.vertex == target
        )
        if not cyclic_reachability:
            return

        # {c ∈ S(a), tc > tb}
        earliest_cyclic = next(iter(cyclic_reachability))
        candidate_reachability = ReverseReachabilitySet(source_reverse_reachability)

        candidate_reachability.prune(
            lower_time_limit=earliest_cyclic.timestamp,
            strictly_smaller=True
        )

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self.reverse_reachability[target] = ReverseReachabilitySet(
            v for v in target_reverse_reachability if v not in cyclic_reachability
        )

        if not candidate_reachability:
            return

        # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
        candidates = Candidates([source])
        candidates.update(c.vertex for c in candidate_reachability)

        if len(candidates) > 1:
            # Output (b, [tb, t], C)
            self.seeds[target][earliest_cyclic.timestamp: current_timestamp] = candidates

        # Periodic pruning  TODO: move outside
        # if time to prune then
        if self.iteration_count % self.prune_interval == 0:
            # for all summaries S(x) do
            for vertex in list(self.reverse_reachability):
                reverse_reachability = self.reverse_reachability[vertex]
                # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
                reverse_reachability.prune(lower_time_limit)
                if not reverse_reachability:
                    del self.reverse_reachability[vertex]

    def _combine_seeds(self, v: Hashable) -> None:
        """
        Merges adjacent or overlapping intervals for vertex `v` within the allowed time window.
        """
        if v not in self.seeds:
            return

        interval_tree = self.seeds[v]
        combined_interval_tree = IntervalTree()

        while len(interval_tree) > 1:
            first_interval_begin = interval_tree.begin()
            upper_interval_limit = first_interval_begin + self.omega  # Define merge range

            compatible_intervals = interval_tree.envelop(first_interval_begin, upper_interval_limit)

            if len(compatible_intervals) == 1:
                compatible_interval = next(iter(compatible_intervals))
                combined_candidates = Candidates(compatible_interval.data)
                combined_interval_end = compatible_interval.end
            else:
                combined_candidates = Candidates()
                combined_interval_end = -np.inf

                for compatible_interval in compatible_intervals:
                    combined_candidates.update(compatible_interval.data)
                    combined_interval_end = max(combined_interval_end, compatible_interval.end)  # Extend interval

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)  # Remove merged intervals
            if interval_tree:
                combined_candidates.next_begin = interval_tree.begin()  # set next interval start
            else:
                combined_candidates.next_begin = upper_interval_limit  # No more intervals

            # Store merged interval
            combined_interval_tree[first_interval_begin: combined_interval_end] = combined_candidates

        if combined_interval_tree:
            self.seeds[v] = combined_interval_tree  # Update seeds

    def _constrained_depth_first_search(self, seed: Seed):
        print(self.transaction_graph.subgraph(seed.candidates).time_slice(seed.begin, seed.end))
        # TODO: add logic for pairs u <-> v
        # search_graph =

    def _get_seed_size_counts(self):
        return dict(Counter(
            len(interval_tree)
            for interval_tree in self.seeds.values()
            if interval_tree
        ))

    def _get_seed_duration_counts(self):
        return dict(Counter(
            interval.length()
            for interval_tree in self.seeds.values() if interval_tree
            for interval in interval_tree
        ))

    def _prune_graph(self):
        minimum_interval_begin = None
        for interval_tree in self.seeds.values():
            if (
                    interval_tree
                    and (
                    (current_interval_begin := interval_tree.begin()) < minimum_interval_begin
                    or minimum_interval_begin is None)
            ):
                minimum_interval_begin = current_interval_begin

    def __iter__(self) -> "GraphCycleIterator":
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop.
        """
        return self

    def __next__(self):
        for source, target, timestamp in self.interactions:
            # Skip trivial self loops
            if source == target:
                continue

            interaction = Interaction(source, target, timestamp)
            lower_time_limit = interaction.timestamp - self.omega

            self.iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(interaction)  # Process new edge

            if self.combine_seeds:
                self._combine_seeds(interaction.target)  # Merge enclosed intervals

            if self.track_history:
                self.vertex_seed_sizes_history.append(self._get_seed_size_counts())
                self.interval_durations_history.append(self._get_seed_duration_counts())

            self.transaction_graph.add_edge(interaction.source, interaction.target, key=interaction.timestamp)

            complete_seeds: list[Seed] = []
            for vertex, interval_tree in self.seeds.items():
                complete_intervals = interval_tree.envelop(0, lower_time_limit)
                if complete_intervals:
                    for interval in complete_intervals:
                        seed = Seed(
                            vertex=vertex,
                            begin=interval.begin,
                            end=interval.end,
                            candidates=interval.data,
                        )
                        complete_seeds.append(seed)
                    interval_tree.remove_envelop(0, lower_time_limit)

            for seed in complete_seeds:
                self._constrained_depth_first_search(seed)

        raise StopIteration()
