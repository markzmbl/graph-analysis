from __future__ import annotations

from _bisect import bisect_right, bisect_left
from collections.abc import Iterable
from typing import Hashable, NamedTuple

Vertex = Hashable
Timestamp = int
TimeDelta = int


class Interaction(NamedTuple):
    source: Vertex
    target: Vertex
    timestamp: Timestamp


class TimedVertex(NamedTuple):
    vertex: Vertex
    time: Timestamp | Iterable[Timestamp]

    def begin(self) -> Timestamp:
        """Returns the minimum time if time is iterable, else returns the time itself."""
        if isinstance(self.time, Iterable):
            return next(iter(self.time))
        return self.time

    def __lt__(self, other: "TimedVertex") -> bool:
        return (self.begin(), self.vertex) < (other.begin(), other.vertex)


class ReachabilitySet(list[TimedVertex]):
    def prune(self, lower_time_limit: Timestamp, strict=True) -> None:
        """
        Return the pruned  reachability set where stale entries are removed.
        Assumes reachability_set is sorted by time_x.
        Uses binary search (`bisect_left`) for efficient pruning.

        :param strict: {(v, t) | t > lower_time_limit}
        """
        # assert sorted(set(self)) == self

        bisect_function = bisect_left if strict else bisect_right

        # Use bisect with key parameter to find the first valid index
        index = bisect_function(self, lower_time_limit, key=lambda item: item.time)

        # for i, (v, t) in enumerate(self):
        #     outside = t <= lower_time_limit if strict else t < lower_time_limit
        #     assert outside and i < index or not outside and i >= index

        # Update the list in place
        del self[:index]

    def __ior__(self, other: ReachabilitySet) -> ReachabilitySet:
        """Merge two sorted lists of unique tuples while preserving order and uniqueness."""
        i, j = 0, 0
        merged = ReachabilitySet()

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

    next_begin: Timestamp | None = None  # Start time of the next interval, if available


class Seed(NamedTuple):
    vertex: Vertex
    begin: Timestamp
    end: Timestamp
    candidates: Candidates
