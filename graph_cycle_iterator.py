from collections import defaultdict
from numbers import Number
from typing import Iterator, Hashable, Set, Dict, Tuple

from intervaltree import IntervalTree, Interval
from sortedcontainers import SortedDict  # third-party library

import dynetx as dn


class Candidates(Set[Hashable]):
    """
    A specialized set to track candidates for cycle formation.
    """
    next_begin: Number | None = None  # Start time of the next interval, if available


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            edge_iterator: Iterator[Tuple[Hashable, Hashable, Number]],
            omega: Number = 10,
            prune_interval: int = 1_000
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self.edge_iterator = edge_iterator  # Stream of edges
        self.iteration_count: int = 0  # Track number of processed edges
        self.prune_interval: int = prune_interval  # Interval for pruning old data
        self.omega: Number = omega  # Maximum time window for relevant edges

        # Reverse reachability mapping: vertex -> set of (predecessor, timestamp) pairs
        self.reverse_reachability: Dict[Hashable, Set[Tuple[Hashable, Number]]] = defaultdict(set)

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self.seeds: Dict[Hashable, IntervalTree] = defaultdict(IntervalTree)

        # Dynamic directed graph with edge removal enabled
        self.dynamic_graph = dn.DynDiGraph(edge_removal=True)

    def get_latest_interval_end(self, v: Hashable, timestamp: Number) -> Number:
        """
        Retrieves the latest interval end time associated with vertex `v` at a given `timestamp`.
        """
        return next(iter(sorted(self.seeds[v][timestamp], reverse=True))).end

    def _prune_reverse_reachability_set(self, vertex: Hashable, current_time_lower_limit: int) -> None:
        """
        Removes stale entries from the reverse reachability set of a given vertex.
        """
        self.reverse_reachability[vertex] = {
            (x, time_x)
            for x, time_x in self.reverse_reachability[vertex]
            if time_x > current_time_lower_limit  # Only keep recent entries
        }

    def _update_reverse_reachability(self, u: Hashable, v: Hashable, current_time: Number) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_time`.
        """
        if u == v:
            return  # Ignore self-loops

        lower_time_limit = current_time - self.omega  # Time limit for relevant edges
        self.reverse_reachability[v].add((u, current_time))  # Add reachability entry

        if u in self.reverse_reachability:
            self._prune_reverse_reachability_set(u, lower_time_limit)  # Prune old entries
            self.reverse_reachability[v].update(self.reverse_reachability[u])  # Propagate reachability

            to_delete = set()
            for w, time_w in self.reverse_reachability[u]:
                if w == v:
                    candidates = {
                        c for c, time_c in self.reverse_reachability[u]
                        if time_c > time_w  # Filter relevant candidates
                    }
                    if candidates:
                        self.seeds[v][time_w: current_time] = Candidates(candidates)
                    to_delete.add((v, time_w))  # Mark for deletion

            self.reverse_reachability[u].difference_update(to_delete)  # Remove old entries

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

    def __iter__(self) -> "GraphCycleIterator":
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop.
        """
        return self

    def run(self) -> None:
        """
        Drives the main loop that processes edges from the underlying edge iterator.
        """
        for u, v, current_time in self.edge_iterator:
            self.iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(u, v, current_time)  # Process new edge

            self._combine_seeds(v)  # Merge overlapping intervals

            expiration_time = self.get_latest_interval_end(v, timestamp=current_time)  # Get expiration
            self.dynamic_graph.add_interaction(u, v, t=current_time, e=expiration_time)  # Update dynamic graph

            if self.iteration_count % self.prune_interval == 0:
                for w in self.reverse_reachability:
                    self._prune_reverse_reachability_set(w, current_time - self.omega)  # Periodic pruning
