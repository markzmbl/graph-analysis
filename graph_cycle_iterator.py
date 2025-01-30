from collections import defaultdict
from numbers import Number
from typing import Iterator, Hashable, Set

from intervaltree import IntervalTree, Interval
from sortedcontainers import SortedDict  # third-party library

import dynetx as dn
class Candidates(set):
    next_begin: Number | None = None

class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT (2-Source
    Cycle Enumeration in Near-real Time) algorithm described by Kumar and Calders.

    The goal is to track possible temporal cycles in a dynamic_graph stream by maintaining
    reverse reachability sets and "seed" intervals. A newly arrived edge at time t
    can connect previously reachable vertices to form cycles. We keep track of these
    potential cycles in an efficient manner by storing and merging intervals.

    Attributes
    ----------
    edge_iterator : Iterator[tuple[Hashable, Hashable, int]]
        An iterator over edges in the form (u, v, t), where:
          - u (Hashable) is the source vertex
          - v (Hashable) is the destination vertex
          - t (int) is the timestamp of the edge's arrival
    omega : int, optional
        The maximum allowed time window when evaluating reachability (default 10).
        If the difference between the current edge's time and a historical edge's
        time is greater than this window, that historical edge is pruned.
    prune_interval : int, optional
        The interval (in terms of number of edges processed) at which pruning
        of stale reachability information is carried out (default 1,000).
    reverse_reachability : defaultdict
        A dictionary mapping each vertex to a set of (predecessor, time) pairs,
        indicating that `predecessor` can reach this vertex at a particular time.
    seeds : defaultdict
        A dictionary mapping each vertex to a sorted collection (IntervalTree)
        of intervals -> candidate sets. Each interval captures a time range where
        a set of vertices might form cycles with that vertex.

    Notes
    -----
    - The 2SCENT algorithm aims to identify cycles in time-evolving graphs
      by leveraging interval constraints and reverse reachability information.
    - This class is written in Python and uses the `sortedcontainers.SortedDict`
      to maintain intervals in a sorted structure.
    - The `sympy` library is used to handle intervals (`Interval`, `FiniteSet`).
    """

    def __init__(
            self,
            edge_iterator: Iterator[tuple[Hashable, Hashable, int]],
            omega: int = 10,
            prune_interval: int = 1_000
    ):
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.

        Parameters
        ----------
        edge_iterator : Iterator[tuple[Hashable, Hashable, int]]
            An iterator over edges of the form (u, v, t).
        omega : int, optional
            The maximum time window for which old edges remain relevant (default 10).
        prune_interval : int, optional
            The number of edges processed between runs of the pruning procedure
            (default 1,000).
        """
        self.edge_iterator = edge_iterator
        self.iteration_count = 0
        self.prune_interval = prune_interval

        # Omega defines how far back in time we consider edges relevant
        self.omega = omega

        # Dictionary: vertex -> set of (predecessor, time_of_arrival)
        self.reverse_reachability = defaultdict(set)

        # Dictionary: vertex -> SortedDict of Interval -> set of candidates
        self.seeds = defaultdict(IntervalTree)

        self.dynamic_graph = dn.DynDiGraph(edge_removal=True)

    def get_latest_interval_end(self, v: Hashable, timestamp: Number) -> Number:
        return next(iter(sorted(self.seeds[v][timestamp], reverse=True))).end

    def _prune_reverse_reachability_set(self, vertex, current_time_lower_limit):
        """
        Removes stale (predecessor, time) entries from the reverse reachability
        set of a given vertex, based on a lower time limit.

        Parameters
        ----------
        vertex : Hashable
            The vertex whose reverse reachability set is being pruned.
        current_time_lower_limit : int
            The oldest time (exclusive) above which edges are still relevant.
            Any (predecessor, time) pair with `time <= current_time_lower_limit`
            will be removed.
        """
        self.reverse_reachability[vertex] = {
            (x, time_x)
            for x, time_x in self.reverse_reachability[vertex]
            if time_x > current_time_lower_limit
        }

    def _update_reverse_reachability(self, u, v, current_time):
        """
        Updates reverse reachability when a new edge (u -> v) arrives at time current_time.

        If v can be reached from u at time current_time, then:
          1. Add (u, current_time) to reverse_reachability[v].
          2. Prune stale entries from u's reverse reachability.
          3. Propagate the union of u's reachable set to v.
          4. Check if adding u -> v creates new intervals in which v might form cycles
             (if v is found in u's reachability at some earlier time).

        Parameters
        ----------
        u : Hashable
            Source vertex of the newly arrived edge.
        v : Hashable
            Destination vertex of the newly arrived edge.
        current_time : int
            Timestamp of the newly arrived edge.
        """
        # Ignore trivial self-loops
        if u == v:
            return

        # Add new reverse reachability link
        lower_time_limit = current_time - self.omega
        self.reverse_reachability[v].add((u, current_time))

        if u in self.reverse_reachability:
            # Prune old edges of u's reverse reachability
            self._prune_reverse_reachability_set(u, lower_time_limit)

            # All nodes reachable to 'u' are also reachable to 'v' now
            self.reverse_reachability[v].update(self.reverse_reachability[u])

            # Identify intervals that might close a cycle
            to_delete = set()
            for w, time_w in self.reverse_reachability[u]:
                if w == v:
                    # Candidates that reached u after w are relevant for intervals
                    candidates = {
                        c for c, time_c in self.reverse_reachability[u]
                        if time_c > time_w
                    }
                    if candidates:
                        # Store the set of candidates in seeds[v] for Interval(time_w, current_time)
                        self.seeds[v][time_w: current_time] = Candidates(candidates)  # Cast to custom set class
                        if time_w < current_time - self.omega:
                            print(1)
                    # Mark (v, time_w) for deletion from reverse_reachability[u]
                    to_delete.add((v, time_w))

            # Remove those edges from u's set to avoid double counting
            self.reverse_reachability[u].difference_update(to_delete)



    def _combine_seeds(self, v):
        """
        Merges adjacent or overlapping intervals for vertex v if they fall
        within a time window of size self.omega, reducing storage and complexity.

        Parameters
        ----------
        v : Hashable
            The vertex whose intervals are being merged.
        """
        interval_tree = self.seeds[v]
        if not interval_tree:
            return
        combined_interval_tree = IntervalTree()

        # SortedDict's keys are intervals, sorted by _interval_sort_key
        # seed_intervals = list(seeds.keys())
        # combined_seeds = SortedDict(self._interval_sort_key)

        # We iteratively merge prefixes of intervals
        while len(interval_tree) > 1:
            first_interval_begin = interval_tree.begin()
            upper_interval_limit = first_interval_begin + self.omega

            # Determine how many intervals fit into [first_interval_start, upper_time_limit]
            prefix_candidates = Candidates()
            prefix_interval_end = 0
            prefix_intervals = interval_tree.envelop(first_interval_begin, upper_interval_limit)

            for _, compatible_interval_end, compatible_interval_candidates in prefix_intervals:
                prefix_candidates.update(compatible_interval_candidates)
                prefix_interval_end = max(prefix_interval_end, compatible_interval_end)

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)
            # or up to upper_time_limit if no more intervals exist
            # The combined interval extends to just before the next interval starts
            if interval_tree:
                prefix_candidates.next_begin = interval_tree.begin()
            else:
                prefix_candidates.next_begin = upper_interval_limit
            combined_interval_tree[first_interval_begin: prefix_interval_end] = prefix_candidates

        # If combined_seeds has been updated, assign it back
        if combined_interval_tree:
            self.seeds[v] = combined_interval_tree

    def __iter__(self):
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop
        or any iterator context.

        Returns
        -------
        GraphCycleIterator
            The iterator itself (Python convention).
        """
        return self

    def run(self):
        """
        Drives the main loop that processes edges from the underlying edge iterator.

        For each incoming edge (u, v, t):
          - Increment the iteration counter.
          - Update the reverse reachability structure (_update_reverse_reachability).
          - Attempt to combine intervals for the destination vertex v.
          - Periodically prune stale entries in the reverse reachability.

        Yields
        ------
        None
            This method currently does not yield anything (though you could
            modify it to yield cycles or potential cycles in the future).
        """


        for u, v, current_time in self.edge_iterator:
            self.iteration_count += 1

            # Update the reverse reachability with the new edge
            self._update_reverse_reachability(u, v, current_time)

            # Combine intervals for v
            self._combine_seeds(v)

            expiration_time = self.get_latest_interval_end(v, timestamp=current_time)
            self.dynamic_graph.add_interaction(u, v, t=current_time, e=expiration_time)

            # Periodically prune the reachability structure
            if self.iteration_count % self.prune_interval == 0:
                # Only prune edges older than (current_time - self.omega)
                for w in self.reverse_reachability:
                    self._prune_reverse_reachability_set(w, current_time - self.omega)
