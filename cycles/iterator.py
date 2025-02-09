from __future__ import annotations

import io
import time
from collections import defaultdict, Counter
from collections.abc import Iterator, Hashable

import numpy as np
import psutil
from intervaltree import IntervalTree
from tqdm.auto import tqdm

from cycles.graph import TransactionGraph
from cycles.types import Vertex, TimeDelta, Interaction, ReverseReachableVertex, ReverseReachabilitySet, \
    Candidates, Seed


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """
    logging: bool = False

    def __init__(
            self,
            interactions: Iterator[Interaction],
            omega: TimeDelta = 10,
            prune_interval: int = 1_000,
            log_stream: io.IOBase | None = None,
            log_interval: int = 1,
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self.interactions = iter(interactions)  # Stream of edges
        self.iteration_count: int = 0  # Track number of processed edges
        self.cleanup_interval: int = prune_interval  # Interval for pruning old data
        self.omega: TimeDelta = omega  # Maximum time window for relevant edges

        # Reverse reachability mapping: vertex -> sorted set of (lower_time_limit, predecessor) pairs
        self.reverse_reachability: dict[Vertex, ReverseReachabilitySet] = defaultdict(ReverseReachabilitySet)

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self.seed_intervals: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)

        # Dynamic directed graph with edge removal enabled
        self.transaction_graph = TransactionGraph()

        if log_stream:
            self.logging = True
            self.start_time = time.monotonic()
            self.last_log_time = self.start_time
            self.process = psutil.Process()

            self.log_interval = log_interval
            self.log_stream = log_stream
            self.log_stream.write("time_seconds,iterations_total,iterations_rate,memory_usage_bytes\n")

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

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(ReverseReachableVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.prune(self.lower_time_limit)

        if source not in self.reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self.reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.prune(self.lower_time_limit)

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
        for cyclic_reachable in cyclic_reachability:
            candidate_reachability = ReverseReachabilitySet(source_reverse_reachability)

            candidate_reachability.prune(
                lower_time_limit=cyclic_reachable.timestamp,
                strictly_smaller=True
            )

            if not candidate_reachability:
                continue

            # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
            candidates = Candidates([source])
            candidates.update(c.vertex for c in candidate_reachability)
            # if candidates <= {59834236, 67975685, 85341516}:
            #     print(1)

            if len(candidates) > 1:
                candidates.next_begin = cyclic_reachable.timestamp + self.omega
                # Output (b, [tb, t], C)
                self.seed_intervals[target][cyclic_reachable.timestamp: current_timestamp] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self.reverse_reachability[target] = ReverseReachabilitySet(
            v for v in target_reverse_reachability if v not in cyclic_reachability
        )

    def _combine_seeds(self, v: Hashable) -> None:
        """
        Merges adjacent or overlapping intervals for vertex `v` within the allowed time window.
        """
        if v not in self.seed_intervals:
            return

        interval_tree = self.seed_intervals[v]
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

            # if combined_candidates <= {59834236, 67975685, 85341516}:
            #     print(1)

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)  # Remove merged intervals
            if interval_tree:
                combined_candidates.next_begin = interval_tree.begin()  # set next interval start
            else:
                combined_candidates.next_begin = upper_interval_limit  # No more intervals

            # Store merged interval
            combined_interval_tree[first_interval_begin: combined_interval_end] = combined_candidates

        if combined_interval_tree:
            self.seed_intervals[v] = combined_interval_tree  # Update seed_intervals

    def _get_seed_size_counts(self):
        return dict(Counter(
            len(interval_tree)
            for interval_tree in self.seed_intervals.values()
            if interval_tree
        ))

    def _get_seed_duration_counts(self):
        return dict(Counter(
            interval.length()
            for interval_tree in self.seed_intervals.values() if interval_tree
            for interval in interval_tree
        ))

    def _get_finalized_seeds(self):
        lower_time_range = (0, self.lower_time_limit)
        finalized_seeds: list[Seed] = []
        for vertex, interval_tree in self.seed_intervals.items():
            for interval in interval_tree.envelop(*lower_time_range):
                seed = Seed(
                    vertex=vertex,
                    begin=interval.begin,
                    end=interval.end,
                    candidates=interval.data,
                )
                finalized_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)
        return finalized_seeds

    def _constrained_depth_first_search(self, seed: Seed):
        # seed.candidates.add(seed.vertex)
        seed.candidates.add(seed.vertex)
        g = self.transaction_graph.subgraph(seed.candidates).time_slice(seed.begin, seed.end)
        n = len(g.nodes())
        m = len(g.edges(keys=True))
        # print(f"nodes: {n}, edges: {m}")
        # if seed.candidates <= {59834236, 67975685, 85341516}:
        #     print(1)
        if m < n:
            print(1)
        if {x for edge in g.edges() for x in edge} < seed.candidates:
            print(1)
        # TODO: add logic for pairs u <-> v
        # search_graph =

    def _prune_reverse_reachability(self):
        # for all summaries S(x) do
        for vertex in list(self.reverse_reachability):
            reverse_reachability = self.reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.prune(self.lower_time_limit)
            if not reverse_reachability:
                del self.reverse_reachability[vertex]

    def _prune_seed_intervals(self):
        for vertex in list(self.seed_intervals):
            if not self.seed_intervals[vertex]:
                del self.seed_intervals[vertex]

    def _prune_transaction_graph(self):
        if self.seed_intervals:
            minimum_interval_begin = min(interval_tree.begin() for interval_tree in self.seed_intervals.values())
            self.transaction_graph.prune(lower_time_limit=minimum_interval_begin)

    def cleanup(self):
        self._prune_reverse_reachability()
        self._prune_seed_intervals()
        self._prune_transaction_graph()

    def log(self):
        current_time = time.monotonic()
        if current_time - self.last_log_time < self.log_interval:
            return

        elapsed_time = current_time - self.start_time
        rate = self.iteration_count / elapsed_time
        memory_usage_bytes = self.process.memory_info().rss

        self.log_stream.write(
            f"{elapsed_time},"
            f"{self.iteration_count},"
            f"{rate},"
            f"{memory_usage_bytes}\n"
        )

    def __iter__(self) -> "GraphCycleIterator":
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop.
        """
        return self

    def __next__(self):
        for source, target, timestamp in tqdm(self.interactions):
            # Skip trivial self loops
            if source == target:
                continue

            interaction = Interaction(source, target, timestamp)
            self.lower_time_limit = interaction.timestamp - self.omega

            self.iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(interaction)  # Process new edge

            self._combine_seeds(interaction.target)  # Merge enclosed intervals

            self.transaction_graph.add_edge(interaction.source, interaction.target, key=interaction.timestamp)

            finalized_seeds = self._get_finalized_seeds()

            if finalized_seeds:
                for seed in finalized_seeds:
                    self._constrained_depth_first_search(seed)

            if self.iteration_count % self.cleanup_interval == 0:
                self.cleanup()

            if self.logging:
                self.log()

        raise StopIteration()
