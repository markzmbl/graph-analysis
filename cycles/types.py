from __future__ import annotations

from _bisect import bisect_right, bisect_left
from typing import Hashable, NamedTuple

Vertex = Hashable
TimeStamp = int
TimeDelta = int


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
