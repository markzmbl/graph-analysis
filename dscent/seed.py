from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from intervaltree import Interval as DataInterval, IntervalTree
from sortedcontainers import SortedDict

from dscent.reachability import DirectReachability
from dscent.types_ import Vertex, Timestamp, Interval, Interaction, SingleTimedVertex, TimeDelta


class Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next data_interval, if available


class RootIntervalTree(IntervalTree[Candidates]):
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
    def construct(root: Vertex, data_interval: DataInterval[Timestamp, Timestamp, Candidates]):
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
    def __init__(self, omega: TimeDelta = 25):
        self._omega: TimeDelta = omega  # Maximum timestamp window for relevant edges

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
