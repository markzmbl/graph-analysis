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


class _CandidateIntervalTree(IntervalTree[_Candidates]):
    def merge_enclosed(self, omega):
        """
        Merges intervals in the tree that are enclosed within a distance threshold.

        Two or more intervals are merged if they are close enough (i.e., their
        `begin` times are within `omega` of each other) and if none of them
        individually stretches equal or beyond the `omega` threshold from the starting point
        of the first interval in the group.

        This function modifies the current IntervalTree in place by creating
        a merged list of intervals, but it does not insert them back into `self`.

        Parameters:
            omega (int or float): Maximum allowed distance from the starting interval's
                                  `begin` value to consider merging others.

        Example:
            If omega = 5 and you have intervals:
            [0, 3], [2, 4], [7, 9], [8, 10]
            → it would merge [0, 4] and [7, 10] separately.

        Note:
            - The method assumes that `iv.data` supports the `|=` operator (e.g., `set`).
            - Final merged intervals are stored in `merged`, but not inserted back
              into the tree. You may want to do `self.clear(); self.update(merged)`
              after the call.
        """
        if not self:
            return

        # Sort by start time; longer intervals first when begins are equal
        ivs = sorted(self, key=lambda iv: (iv.begin, -iv.end))
        merged = []

        # Start with the first interval
        iv = ivs.pop(0)
        while ivs:
            begin = iv.begin
            end = iv.end
            data = iv.data
            upper_limit = begin + omega  # Threshold for merging

            # Merge as long as next intervals fall within the upper_limit
            while len(ivs) > 0:
                next_iv = ivs.pop(0)
                if next_iv.end >= upper_limit:
                    # This interval is too far or large; start a new group
                    iv = next_iv
                    break
                # Extend the merged interval
                end = max(end, next_iv.end)
                data |= next_iv.data

            # Store the merged interval
            merged.append(DataInterval(begin, end, data))

        # Add the last remaining interval that wasn’t appended in the loop
        merged.append(iv)

        self.__init__(merged)


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


class _ReverseReachabilityManager:
    pass


class SeedGenerator:
    def __init__(self, omega: TimeDelta = 10):
        self._omega: TimeDelta = omega  # Maximum timestamp window for relevant edges
        self._lower_limit = 0

        # Reverse reachability mapping: root -> sorted set of (_lower_limit, predecessor) pairs
        self._reverse_reachability: dict[Vertex, DirectReachability] = defaultdict(DirectReachability)
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._root_interval_trees: dict[Vertex, _CandidateIntervalTree] = defaultdict(_CandidateIntervalTree)
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
                new_seeds = True  # Set Flag
                # Output (b, [tb, t], C)
                seed_range = slice(cyclic_reachable.timestamp, current_timestamp)
                self._root_interval_trees[target][seed_range] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = DirectReachability(
            reverse_reachable
            for reverse_reachable in target_reverse_reachability
            if reverse_reachable.vertex != target
        )

        return new_seeds

    def _combine_seeds(self, v: Vertex) -> None:
        """
        Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
        """
        interval_tree = self._root_interval_trees[v]  # Get the interval tree for vertex `v`
        if len(interval_tree) < 2:
            return
        interval_tree.merge_enclosed(self._omega)

    def update(self, interaction: Interaction):
        # Update reverse reachability, check if new seeds are added
        seeds_updated = self._update_reverse_reachability(interaction)  # Process new edge
        if seeds_updated:
            self._combine_seeds(interaction.target)  # Merge enclosed intervals

    def get_primed_seeds(self, vertex: Vertex, upper_limit: Timestamp) -> list[Seed]:
        # Initialize a list to store primed seeds
        primed_seeds: list[Seed] = []
        # Iterate over each root and its associated interval tree
        for root, interval_tree in self._root_interval_trees.items():
            # Check if the interval tree contains intervals starting before or at the cutoff time
            if interval_tree.begin() <= upper_limit - self._omega:
                if upper_limit < interval_tree.end():
                    complete = False  # Flag to track if the interval tree is fully processed
                    # Get only the intervals that are fully within the time window
                    primed_intervals = interval_tree.envelop(begin=interval_tree.begin(), end=upper_limit)
                else:
                    complete = True
                    primed_intervals = list(interval_tree)
                # Convert each qualifying interval into a Seed object and collect them
                for primed_interval in primed_intervals:
                    primed_seeds.append(Seed.construct(root=root, data_interval=primed_interval))
                    # Convert each qualifying interval into a Seed object and collect them
                    if not complete:
                        interval_tree.remove(primed_interval)
                # If all intervals have been processed, clear the entire tree
                if complete:
                    interval_tree.clear()
            # Remove the root from the tree dictionary if its interval tree is now empty
            if len(interval_tree) == 0:
                del self._root_interval_trees[root]
        # Return the list of gathered seeds
        return primed_seeds

    def cleanup(self):
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
