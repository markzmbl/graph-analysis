from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Callable
from numbers import Real
from typing import Iterator, Hashable, Set, Dict, Tuple
import heapq

import numpy as np
from line_profiler import profile

EPS = 1  #np.finfo(float).eps
from intervaltree import IntervalTree, Interval

import dynetx as dn

ReverseReachabilitySet = list[Tuple[Real, Hashable]]


class Candidates(Set[Hashable]):
    """
    A specialized set to track candidates for cycle formation.
    """
    next_begin: Real | None = None  # Start time of the next interval, if available


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            edge_iterator: Iterator[Tuple[Hashable, Hashable, Real]],
            omega: Real = 10,
            prune_interval: int = 1_000
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self.edge_iterator = edge_iterator  # Stream of edges
        self.iteration_count: int = 0  # Track number of processed edges
        self.prune_interval: int = prune_interval  # Interval for pruning old data
        self.omega: Real = omega  # Maximum time window for relevant edges

        # Reverse reachability mapping: vertex -> sorted set of (lower_time_limit, predecessor) pairs
        self.reverse_reachability: Dict[Hashable, ReverseReachabilitySet] = defaultdict(list)

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self.seeds: Dict[Hashable, IntervalTree] = defaultdict(IntervalTree)

        # Dynamic directed graph with edge removal enabled
        self.dynamic_graph = dn.DynDiGraph(edge_removal=True)

    # @staticmethod
    # def _get_latest_seed_interval_end(interval_tree: IntervalTree, timestamp: Real) -> Real:
    #     """
    #     Retrieves the latest interval end time associated with vertex `v` at a given `lower_time_limit`.
    #     """
    #     return next(iter(sorted(interval_tree[timestamp], reverse=True))).end

    @staticmethod
    def _get_pruned_reverse_reachability_set(
            reverse_reachability_set: ReverseReachabilitySet,
            lower_time_limit: Real
    ) -> ReverseReachabilitySet:
        """
        Return the pruned reverse reachability set where stale entries are removed.
        Assumes reverse_reachability_set is sorted by time_x.
        Uses binary search (`bisect_left`) for efficient pruning.
        """
        # Use bisect_left with key parameter to find the first valid index
        index = bisect_right(  # type: ignore
            reverse_reachability_set, lower_time_limit,
            key=lambda item: item[0]
        )

        # Slice off stale entries
        return reverse_reachability_set[index:]

    @staticmethod
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

    def _update_reverse_reachability(self, u: Hashable, v: Hashable, current_timestamp: Real) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        if u == v:
            return  # Ignore self-loops

        # Time limit for relevant edges
        lower_time_limit: Real = current_timestamp - self.omega  # type: ignore
        # Add reachability entry
        self.reverse_reachability[v].append((current_timestamp, u))


        if u in self.reverse_reachability:
            # Prune old entries
            self.reverse_reachability[u] = self._get_pruned_reverse_reachability_set(
                self.reverse_reachability[u], lower_time_limit
            )
            # Propagate reachability
            tmp = list(self.reverse_reachability[v])
            self.reverse_reachability[v] = self._merge_sorted_sets(
                self.reverse_reachability[v], self.reverse_reachability[u]
            )


            to_delete = []
            for time_w, w in self.reverse_reachability[u]:
                if w == v and time_w < current_timestamp:
                    # Prune stale entries
                    looped_reverse_reachability = self._get_pruned_reverse_reachability_set(
                        self.reverse_reachability[u], lower_time_limit=time_w
                    )
                    if looped_reverse_reachability:
                        # Extract the node ids
                        candidates = [candidate for _, candidate in looped_reverse_reachability]
                        self.seeds[v][time_w: current_timestamp] = candidates
                    to_delete.append((time_w, v))

            # Remove to avoid duplicate output
            if to_delete:
                self.reverse_reachability[u] = [w for w in self.reverse_reachability[u] if w not in to_delete]




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
            upper_interval_limit = first_interval_begin + self.omega  # Define merge range

            prefix_candidates = Candidates()
            prefix_interval_end = 0
            prefix_intervals = interval_tree.envelop(first_interval_begin, upper_interval_limit)

            for _, compatible_interval_end, compatible_interval_candidates in prefix_intervals:
                prefix_candidates.update(compatible_interval_candidates)
                prefix_interval_end = max(prefix_interval_end, compatible_interval_end)  # Extend interval

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)  # Remove merged intervals

            if interval_tree:
                prefix_candidates.next_begin = interval_tree.begin()  # Set next interval start
            else:
                prefix_candidates.next_begin = upper_interval_limit  # No more intervals

            combined_interval_tree[
            first_interval_begin: prefix_interval_end] = prefix_candidates  # Store merged interval

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
        for u, v, current_timestamp in self.edge_iterator:
            self.iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(u, v, current_timestamp=current_timestamp)  # Process new edge

            self._combine_seeds(v)  # Merge enclosed intervals

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
                lower_time_limit = current_timestamp - self.omega
                for w in self.reverse_reachability:
                    self._get_pruned_reverse_reachability_set(self.reverse_reachability[w], lower_time_limit)
