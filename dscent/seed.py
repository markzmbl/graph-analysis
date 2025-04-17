from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from intervaltree import Interval as DataInterval, IntervalTree
from sortedcontainers import SortedDict

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


class _SeedRegistry:
    def __init__(self):
        self._intervals: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)
        self._ends: dict[Timestamp, Interval] = SortedDict()

    def add(self, root: Vertex, begin: Timestamp, end: Timestamp, candidates: _Candidates):
        interval = DataInterval(begin, end, candidates)
        self._intervals[root].add(interval)
        self._ends[end] = interval
        pass

    def combine_seeds(self, v: Vertex) -> None:
        """
        Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
        """
        interval_tree = self._seed_intervals[v]  # Get the interval tree for vertex `v`
        if len(interval_tree) < 2:
            return

        combined_interval_tree = IntervalTree()  # Initialize a new interval tree for merged intervals

        while len(interval_tree) > 0:
            # Get the first interval
            interval_begin = interval_tree.begin()
            upper_interval_limit = interval_begin + self._omega  # Define merge range
            lower_time_range = (0, upper_interval_limit)

            # Find all compatible intervals within the range
            compatible_intervals = interval_tree.envelop(*lower_time_range)
            assert len(compatible_intervals) > 0

            # Merge compatible intervals
            combined_candidates = _Candidates()
            combined_interval_end = float("-inf")

            # Create union of compatible candidates
            # Determine the maximum upper_limit timestamp
            for compatible_interval in compatible_intervals:
                combined_candidates.update(compatible_interval.data)
                if compatible_interval.end > combined_interval_end:
                    combined_interval_end = compatible_interval.end

            interval_tree.remove_envelop(*lower_time_range)  # Remove merged intervals
            combined_candidates.next_begin = (  # set next data_interval start
                interval_tree.begin() if interval_tree
                else upper_interval_limit
            )
            # Store merged data_interval
            combined_interval_tree[interval_begin: combined_interval_end] = combined_candidates

        self._seed_intervals[v] = combined_interval_tree  # Update _seed_intervals


class SeedGenerator:
    def __init__(self, omega: TimeDelta = 10):
        self._omega: TimeDelta = omega  # Maximum timestamp window for relevant edges
        self._lower_limit = 0

        # Reverse reachability mapping: root -> sorted set of (_lower_limit, predecessor) pairs
        self._reverse_reachability: dict[Vertex, DirectReachability] = defaultdict(DirectReachability)
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._seed_registry: _SeedRegistry = _SeedRegistry()
        # Seeds which can not grow any further and are primed for exploration
        self._primed_seeds: list[Seed] = []

    def _update_reverse_reachability(self, interaction: Interaction) -> bool:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.

        :param interaction: (a, b, t)
            where 'a' is the source vertex, 'b' is the target vertex, and 't' is the timestamp.
        :return: True if new seeds are added, False otherwise.
        """
        # (a, b, t) ∈ E
        source, target, current_timestamp = interaction
        target_reverse_reachability = self._reverse_reachability[target]

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(SingleTimedVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.trim_before(self._lower_limit, strict=False)

        if source not in self._reverse_reachability:
            return False

        # if S(a) exists then
        source_reverse_reachability = self._reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.trim_before(self._lower_limit, strict=False)

        # Output flag to indicate if new seeds are added
        new_seeds = False

        if len(source_reverse_reachability) == 0:
            del self._reverse_reachability[source]
            return False

        # Propagate reachability
        # S(b) ← S(b) ∪ S(a)
        target_reverse_reachability |= source_reverse_reachability

        # for (b, tb) ∈ S(b) do
        cyclic_reachability = DirectReachability([
            v for v in target_reverse_reachability
            if v.vertex == target and v.timestamp < current_timestamp
        ])
        if len(cyclic_reachability) == 0:
            return False

        # {c ∈ S(a), tc > tb}
        for cyclic_reachable in cyclic_reachability:
            try:
                assert (
                        current_timestamp - self._omega <= cyclic_reachable.timestamp < current_timestamp
                ), f"{cyclic_reachable.timestamp=} {current_timestamp=}"
            except AssertionError:
                print(1)

            # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
            candidate_reachability = DirectReachability(source_reverse_reachability)
            candidates = _Candidates([source])
            candidates.update(c.vertex for c in candidate_reachability)

            if len(candidates) > 1:
                new_seeds = True
                candidates.next_begin = cyclic_reachable.timestamp + self._omega  # Default next_begin
                # Output (b, [tb, t], C)
                self._seed_registry.add(
                    root=target,
                    begin=cyclic_reachable.timestamp,
                    end=current_timestamp,
                    candidates=candidates,
                )

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = DirectReachability(
            reverse_reachable
            for reverse_reachable in target_reverse_reachability
            if reverse_reachable.vertex != target
        )

        return new_seeds



    def update(self, interaction: Interaction):
        # Update the lower time limit for the current interaction
        lower_time_limit = interaction.timestamp - self._omega
        assert self._lower_limit <= lower_time_limit
        self._lower_limit = lower_time_limit
        # Update reverse reachability, check if new seeds are added
        seeds_updated = self._update_reverse_reachability(interaction)  # Process new edge
        if seeds_updated:
            self._combine_seeds(interaction.target)  # Merge enclosed intervals
            self._update_primed_seeds(interaction.target, upper_limit=self._lower_limit)  # Update primed seeds

    def _prune_reverse_reachability(self):
        to_delete: list[Vertex] = []
        # for all summaries S(x) do
        for vertex in self._reverse_reachability:
            reverse_reachability = self._reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.trim_before(self._lower_limit)
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

    def _update_primed_seeds(self, vertex: Vertex, upper_limit: Timestamp):
        interval_tree = self._seed_intervals[vertex]
        if upper_limit > interval_tree.begin():
            lower_time_range = (0, upper_limit)
            for data_interval in interval_tree.envelop(*lower_time_range):
                seed = Seed.construct(root=vertex, data_interval=data_interval)
                self._primed_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)

    def update_primed_seeds(self, complete: bool = False):
        upper_limit = self._lower_limit if complete else float("inf")
        for root_vertex in self._seed_intervals:
            self._update_primed_seeds(root_vertex, upper_limit=upper_limit)

    def has_seeds(self) -> bool:
        return bool(self._primed_seeds)

    def pop_seed(self) -> Seed:
        return self._primed_seeds.pop()

    @property
    def seeds(self) -> list[Seed]:
        return self._primed_seeds
