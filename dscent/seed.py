from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from intervaltree import Interval as DataInterval, IntervalTree

from dscent.reachability import DirectReachability
from dscent.types_ import Vertex, Timestamp, Interval, Interaction, SingleTimedVertex, TimeDelta


class _Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next data_interval, if available


@dataclass(frozen=True)
class Seed:
    root: Vertex
    interval: Interval
    candidates: frozenset[Vertex]
    next_begin: Timestamp

    @staticmethod
    def construct(root: Vertex, data_interval: DataInterval[Timestamp, Timestamp, _Candidates]):
        begin, end, candidates = data_interval
        candidates.add(root)
        next_begin = candidates.next_begin

        return Seed(
            root=root,
            interval=Interval(begin, end),
            candidates=frozenset(candidates),
            next_begin=next_begin,
        )


class SeedGenerator:
    def __init__(self, omega: TimeDelta = 10):
        self._omega: TimeDelta = omega  # Maximum timestamp window for relevant edges
        self._lower_time_limit = 0

        # Reverse reachability mapping: root -> sorted set of (_lower_time_limit, predecessor) pairs
        self._reverse_reachability: dict[Vertex, DirectReachability] = defaultdict(DirectReachability)
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._seed_intervals: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)
        # Seeds which can not grow any further and are primed for exploration
        self._primed_seeds: list[Seed] = []

    def _update_reverse_reachability(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        # (a, b, t) ∈ E
        source, target, current_timestamp = interaction
        lower_time_limit = current_timestamp - self._omega
        assert self._lower_time_limit <= lower_time_limit
        self._lower_time_limit = lower_time_limit
        target_reverse_reachability = self._reverse_reachability[target]

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(SingleTimedVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.trim_before(self._lower_time_limit, strict=False)

        if source not in self._reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self._reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.trim_before(self._lower_time_limit, strict=False)
        if len(source_reverse_reachability) == 0:
            del self._reverse_reachability[source]
            return

        # Propagate reachability
        # S(b) ← S(b) ∪ S(a)
        target_reverse_reachability |= source_reverse_reachability

        # for (b, tb) ∈ S(b) do
        cyclic_reachability = DirectReachability(
            [v for v in target_reverse_reachability if v.vertex == target]
        )
        if len(cyclic_reachability) == 0:
            return

        # {c ∈ S(a), tc > tb}
        for cyclic_reachable in cyclic_reachability:
            candidate_reachability = DirectReachability(source_reverse_reachability)
            candidate_reachability.trim_before(lower_limit=cyclic_reachable.timestamp)

            if len(candidate_reachability) == 0:
                continue

            # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
            candidates = _Candidates([source])
            candidates.update(c.vertex for c in candidate_reachability)

            if len(candidates) > 1:
                candidates.next_begin = cyclic_reachable.timestamp + self._omega
                # Output (b, [tb, t], C)
                self._seed_intervals[target][cyclic_reachable.timestamp: current_timestamp] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = DirectReachability(
            reverse_reachable
            for reverse_reachable in target_reverse_reachability
            if reverse_reachable.vertex not in cyclic_reachability.vertices
        )

    def _combine_seeds(self, v: Vertex) -> None:
        """
        Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
        """
        if v not in self._seed_intervals:
            return

        interval_tree = self._seed_intervals[v]
        combined_interval_tree = IntervalTree()

        while len(interval_tree) > 1:
            interval_begin = interval_tree.begin()
            upper_interval_limit = interval_begin + self._omega  # Define merge range

            compatible_intervals = interval_tree.envelop(0, upper_interval_limit)

            combined_candidates = _Candidates()
            combined_interval_end = float("-inf")

            for compatible_interval in compatible_intervals:
                combined_candidates.update(compatible_interval.data)
                combined_interval_end = max(combined_interval_end, compatible_interval.end)  # Extend data_interval

            interval_tree.remove_envelop(0, upper_interval_limit)  # Remove merged intervals
            combined_candidates.next_begin = (  # set next data_interval start
                interval_tree.begin() if interval_tree
                else upper_interval_limit
            )
            # Store merged data_interval
            combined_interval_tree[interval_begin: combined_interval_end] = combined_candidates

        if combined_interval_tree:
            self._seed_intervals[v] = combined_interval_tree  # Update _seed_intervals

    def update(self, interaction: Interaction):
        self._update_reverse_reachability(interaction)  # Process new edge
        self._combine_seeds(interaction.target)  # Merge enclosed intervals
        self.update_primed_seeds(complete=False)

    def _prune_reverse_reachability(self):
        to_delete: list[Vertex] = []
        # for all summaries S(x) do
        for vertex in self._reverse_reachability:
            reverse_reachability = self._reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.trim_before(self._lower_time_limit)
            if len(reverse_reachability) == 0:
                to_delete.append(vertex)

        for vertex in to_delete:
            del self._reverse_reachability[vertex]

    def _clean_seed_intervals(self):
        to_delete: list[Vertex] = [
            vertex
            for vertex, seed_interval
            in self._seed_intervals.items()
            if seed_interval.is_empty()
        ]
        for vertex in to_delete:
            del self._seed_intervals[vertex]

    def cleanup(self):
        self._prune_reverse_reachability()
        self._clean_seed_intervals()

    def update_primed_seeds(self, complete: bool = False):
        end = float("inf") if complete else self._lower_time_limit
        lower_time_range = (0, end)
        for vertex, interval_tree in self._seed_intervals.items():
            for data_interval in interval_tree.envelop(*lower_time_range):
                seed = Seed.construct(root=vertex, data_interval=data_interval)
                self._primed_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)

    def has_seeds(self) -> bool:
        return bool(self._primed_seeds)

    def pop_seed(self) -> Seed:
        return self._primed_seeds.pop()

    @property
    def seeds(self) -> list[Seed]:
        return self._primed_seeds
