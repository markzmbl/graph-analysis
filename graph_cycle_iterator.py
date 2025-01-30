from bisect import bisect_right
from collections import defaultdict
from numbers import Real
from typing import Dict, Hashable, Iterator, NamedTuple, Set

import dynetx as dn
from intervaltree import IntervalTree
from line_profiler import profile

Vertex = Hashable
TimeStamp = int
TimeDelta = int


class Interaction(NamedTuple):
    source: Vertex
    target: Vertex
    timestamp: TimeStamp


class ReverseReachableVertex(NamedTuple):
    timestamp: TimeStamp
    vertex: Vertex


ReverseReachabilitySet = list[ReverseReachableVertex]


class Candidates(Set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: TimeStamp | None = None  # Start time of the next interval, if available


def _get_pruned_reverse_reachability_set(
    reverse_reachability_set: ReverseReachabilitySet, lower_time_limit: TimeStamp
) -> ReverseReachabilitySet:
    """
    Return the pruned reverse reachability set where stale entries are removed.
    Assumes reverse_reachability_set is sorted by time_x.
    Uses binary search (`bisect_left`) for efficient pruning.
    """
    # Use bisect_left with key parameter to find the first valid index
    index = bisect_right(
        reverse_reachability_set, lower_time_limit, key=lambda item: item.timestamp
    )

    # Slice off stale entries
    return reverse_reachability_set[index:]


def _merge_sorted_sets(list1, list2):
    """Merge two sorted lists of unique tuples while preserving order and uniqueness."""
    i, j = 0, 0
    merged = []

    while i < len(list1) and j < len(list2):
        if list1[i] < list2[j]:
            merged.append(list1[i])
            i += 1
        elif list2[j] < list1[i]:
            merged.append(list2[j])
            j += 1
        else:
            # They are equal, add only one copy
            merged.append(list1[i])
            i += 1
            j += 1

    # Append remaining elements (if any)
    merged.extend(list1[i:])
    merged.extend(list2[j:])

    return merged


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
        self,
        interactions: Iterator[Interaction],
        omega: TimeDelta = 10,
        prune_interval: int = 1_000,
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self.interactions = interactions  # Stream of edges
        self.iteration_count: int = 0  # Track number of processed edges
        self.prune_interval: int = prune_interval  # Interval for pruning old data
        self.omega: TimeDelta = omega  # Maximum time window for relevant edges

        # Reverse reachability mapping: vertex -> sorted set of (lower_time_limit, predecessor) pairs
        self.reverse_reachability: Dict[Vertex, ReverseReachabilitySet] = defaultdict(
            list
        )

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self.seeds: Dict[Vertex, IntervalTree] = defaultdict(IntervalTree)

        # Dynamic directed graph with edge removal enabled
        self.dynamic_graph = dn.DynDiGraph(edge_removal=True)

    def _update_reverse_reachability(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        source, target, current_timestamp = (
            interaction.source,
            interaction.target,
            interaction.timestamp,
        )

        if source == target:
            return  # Ignore self-loops

        # Time limit for relevant edges
        lower_time_limit: TimeStamp = current_timestamp - self.omega
        # Add reachability entry
        reverse_reachable_source = ReverseReachableVertex(current_timestamp, source)
        self.reverse_reachability[target].append(reverse_reachable_source)

        if source in self.reverse_reachability:
            # Prune old entries
            self.reverse_reachability[source] = _get_pruned_reverse_reachability_set(
                self.reverse_reachability[source], lower_time_limit
            )
            # Propagate reachability
            self.reverse_reachability[target] = _merge_sorted_sets(
                self.reverse_reachability[target], self.reverse_reachability[source]
            )

            to_delete = []
            for vertex_timestamp, vertex in self.reverse_reachability[source]:
                if vertex == target and vertex_timestamp < current_timestamp:
                    # Prune stale entries
                    looped_reverse_reachability = _get_pruned_reverse_reachability_set(
                        self.reverse_reachability[source],
                        lower_time_limit=vertex_timestamp,
                    )
                    if looped_reverse_reachability:
                        # Extract the vertex ids
                        candidates = [
                            candidate.vertex
                            for candidate in looped_reverse_reachability
                        ]
                        self.seeds[target][
                            vertex_timestamp:current_timestamp
                        ] = candidates
                    to_delete.append((vertex_timestamp, target))

            # Remove to avoid duplicate output
            if to_delete:
                self.reverse_reachability[source] = [
                    v for v in self.reverse_reachability[source] if v not in to_delete
                ]

    def _combine_seeds(self, v: Hashable) -> None:
        """
        Merges adjacent or overlapping intervals for vertex `v` within the allowed time window.
        """
        interval_tree = self.seeds[v]
        if not interval_tree:
            return

        combined_interval_tree = IntervalTree()

        while len(interval_tree) > 1:
            first_interval_begin = interval_tree.begin()
            upper_interval_limit = (
                first_interval_begin + self.omega
            )  # Define merge range

            prefix_candidates = Candidates()
            prefix_interval_end = 0
            prefix_intervals = interval_tree.envelop(
                first_interval_begin, upper_interval_limit
            )

            for (
                _,
                compatible_interval_end,
                compatible_interval_candidates,
            ) in prefix_intervals:
                prefix_candidates.update(compatible_interval_candidates)
                prefix_interval_end = max(
                    prefix_interval_end, compatible_interval_end
                )  # Extend interval

            interval_tree.remove_envelop(
                first_interval_begin, upper_interval_limit
            )  # Remove merged intervals

            if interval_tree:
                prefix_candidates.next_begin = (
                    interval_tree.begin()
                )  # Set next interval start
            else:
                prefix_candidates.next_begin = upper_interval_limit  # No more intervals

            combined_interval_tree[first_interval_begin:prefix_interval_end] = (
                prefix_candidates  # Store merged interval
            )

        if combined_interval_tree:
            self.seeds[v] = combined_interval_tree  # Update seeds

    def _constrained_depth_first_search(self, v: Hashable):
        pass

    def __iter__(self) -> "GraphCycleIterator":
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop.
        """
        return self

    @profile
    def run(self) -> None:
        """
        Drives the main loop that processes edges from the underlying edge iterator.
        """
        for source, target, timestamp in self.interactions:
            interaction = Interaction(source, target, timestamp)
            self.iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(interaction)  # Process new edge

            self._combine_seeds(interaction.target)  # Merge enclosed intervals

            # # Get expiration
            # expiration_time = self._get_latest_seed_interval_end(self.seeds[v], timestamp=current_timestamp)
            # Update dynamic graph
            # self.dynamic_graph.add_interaction(u, v, t=current_timestamp, e=expiration_time)
            # self.dynamic_graph.add_interaction(u, v, t=current_timestamp)

            # for w, interval_tree in self.seeds.items():
            #     if not interval_tree:
            #         continue
            #     # self._constrained_depth_first_search(v)

            # Periodic pruning
            if self.iteration_count % self.prune_interval == 0:
                lower_time_limit = interaction.timestamp - self.omega
                for w in self.reverse_reachability:
                    _get_pruned_reverse_reachability_set(
                        self.reverse_reachability[w], lower_time_limit
                    )
