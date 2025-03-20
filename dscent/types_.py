from __future__ import annotations

from _bisect import bisect_right, bisect_left
from abc import ABC
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Hashable, NamedTuple

Vertex = Hashable
Timestamp = int
TimeDelta = int


class Interaction(NamedTuple):
    source: Vertex
    target: Vertex
    timestamp: Timestamp


class TimeSequence(list[Timestamp]):
    """A sorted sequence of timestamps with efficient trimming methods."""

    def __init__(self, timestamps: Iterable[Timestamp] | None = None):
        """
        Initializes a sorted sequence of timestamps.

        :param timestamps: A list of datetime objects.
        """
        if timestamps is None:
            timestamps = []
        assert list(timestamps) == sorted(timestamps), "All elements must sorted"
        super().__init__(timestamps)  # Ensure the list is always sorted

    def begin(self) -> Timestamp:
        """
        Returns the first timestamp in the sequence.
        Raises a ValueError if the sequence is empty.
        """
        try:
            return next(iter(self))
        except StopIteration:
            raise ValueError("TimeSequence is empty, cannot retrieve the first element.")

    def end(self) -> Timestamp:
        """
        Returns the last timestamp in the sequence.
        Raises a ValueError if the sequence is empty.
        """
        try:
            return next(reversed(self))
        except StopIteration:
            raise ValueError("TimeSequence is empty, cannot retrieve the last element.")

    def get_trim_index(self, limit: Timestamp, strict: bool, left: bool) -> int:
        """
        Returns the appropriate index for trimming operations using bisect.

        :param limit: The threshold timestamp.
        :param strict: Determines whether to exclude the limit itself.
        :param left: If True, trims from the left (before limit). Otherwise, trims from the right (after limit).
        :return: The index to slice the list.
        """
        if left:
            return bisect_right(self, limit) if strict else bisect_left(self, limit)
        else:
            return bisect_left(self, limit) if strict else bisect_right(self, limit)

    def trim_before(self, lower_limit: Timestamp, strict: bool = True) -> TimeSequence:
        """
        Removes timestamps before the given upper_limit.

        :param upper_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly before the upper_limit.
                       If False, removes elements before or equal to the upper_limit.
        :return: A new TimeSequence with the filtered timestamps.
        """
        idx = self.get_trim_index(lower_limit, strict, left=True)
        del self[: idx]

    def trim_after(self, upper_limit: Timestamp, strict: bool = True) -> TimeSequence:
        """
        Removes timestamps after the given upper_limit.

        :param upper_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly after the upper_limit.
                       If False, removes elements after or equal to the upper_limit.
        :return: A new TimeSequence with the filtered timestamps.
        """
        idx = self.get_trim_index(upper_limit, strict, left=False)
        del self[idx:]


@dataclass
class TimedVertexABC(ABC):
    vertex: Vertex

    def begin(self) -> Timestamp:
        """Returns the minimum timestamp."""
        raise NotImplementedError

    def end(self) -> Timestamp:
        """Returns the maximum timestamp."""
        raise NotImplementedError

    def __lt__(self, other: TimedVertexABC) -> bool:
        return (self.begin(), self.vertex) < (other.begin(), other.vertex)


@dataclass
class SingleTimedVertex(TimedVertexABC):
    timestamp: Timestamp

    def begin(self) -> Timestamp:
        return self.timestamp

    def end(self):
        return self.timestamp

    def __str__(self):
        return f"({self.vertex}, {repr(self.timestamp)})"


@dataclass
class MultiTimedVertex(TimedVertexABC):
    timestamps: TimeSequence

    def begin(self) -> Timestamp:
        return self.timestamps.begin()

    def end(self):
        return self.timestamps.end()

    def __str__(self):
        return f"({self.vertex}, [{", ".join(map(str, self.timestamps))}])"


class ReachabilitySet:
    vertices: list[Vertex]
    timestamps: list[Timestamp]

    def __init__(
            self,
            timed_vertices: Iterable[SingleTimedVertex] | ReachabilitySet | None = None,
            vertices: list[Vertex] | None = None,
            timestamps: list[Timestamp] | TimeSequence | None = None
    ):
        if vertices is None:
            vertices = []
        if timestamps is None:
            timestamps = TimeSequence()

        if timed_vertices is not None:
            if isinstance(timed_vertices, ReachabilitySet):
                vertices = timed_vertices.vertices
                timestamps = timed_vertices.timestamps
            else:
                for timed_vertex in timed_vertices:
                    vertices.append(timed_vertex.vertex)
                    timestamps.append(timed_vertex.timestamp)

        assert timestamps is None or vertices is None or len(list(vertices)) == len(timestamps), (
            f"Mismatch between number of vertices {vertices} and timestamps {timestamps}."
        )
        if not isinstance(timestamps, TimeSequence):
            assert sorted(timestamps) == timestamps, (
                f"Timestamps must be in sorted order when not an instance of TimeSequence. Provided: {timestamps}"
            )
        self.vertices = vertices
        self.timestamps = timestamps

    def get_trim_index(self, limit: Timestamp, strict: bool, left: bool) -> int:
        return self.timestamps.get_trim_index(limit=limit, strict=strict, left=left)

    def trim_before(self, lower_limit: Timestamp, strict=True) -> None:
        """
        Return the pruned  reachability set where stale entries are removed.

        :param upper_limit:
        :param strict: {(v, t) | t > upper_limit}
        """
        idx = self.get_trim_index(lower_limit, strict, left=True)
        del self.vertices[: idx]
        del self.timestamps[: idx]

    def trim_after(self, upper_limit: Timestamp, strict=True) -> None:
        """
        Return the pruned reachability set where most recent entries are removed.

        :param upper_limit:
        :param strict: {(v, t) | t < upper_limit}
        """
        idx = self.get_trim_index(upper_limit, strict, left=False)
        del self.vertices[: idx]
        del self.timestamps[: idx]

    def append(self, item: SingleTimedVertex):
        """Appends a new vertex and its timestamp."""
        self.vertices.append(item.vertex)
        self.timestamps.append(item.timestamp)

    def extend(self, other: ReachabilitySet):
        """Extends the set with another ReachabilitySet."""
        self.vertices.extend(other.vertices)
        self.timestamps.extend(other.timestamps)

    def __len__(self):
        """Returns the number of stored elements."""
        return len(self.vertices)

    def __getitem__(self, index: int | slice) -> SingleTimedVertex | ReachabilitySet:
        """Allows indexed access to paired (vertex, timestamp) tuples."""
        if isinstance(index, slice):
            return ReachabilitySet(vertices=self.vertices[index], timestamps=self.timestamps[index])
        return SingleTimedVertex(self.vertices[index], self.timestamps[index])

    def __iter__(self) -> Iterable[SingleTimedVertex]:
        """Enables iteration, returning SingleTimedVertex objects."""
        return (
            SingleTimedVertex(vertex, timestamp)
            for vertex, timestamp
            in zip(self.vertices, self.timestamps)
        )

    def __delitem__(self, index: int | slice):
        """Deletes elements by index or slice."""
        del self.vertices[index]
        del self.timestamps[index]

    def __or__(self, other: ReachabilitySet) -> ReachabilitySet:
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

        return merged


class BundledPath(list[MultiTimedVertex]):
    def append(self, item: MultiTimedVertex):
        if len(self) > 0:
            *_, head = self
            idx = (
                item.timestamps.get_trim_index(head.begin(), strict=True, left=True)
                if item.begin() > head.begin()
                else 0
            )
            item.timestamps = TimeSequence(item.timestamps[idx:])  # Copy and trim
            if len(item.timestamps) == 0:
                raise ValueError
        else:
            item.timestamps = TimeSequence(item.timestamps)  # Copy

        super().append(item)
        for predecessor, successor in reversed(list(zip(self[:-1], self[1:]))):
            # Check if predecessor timestamps need trimming
            if predecessor.end() > successor.end():
                idx = predecessor.timestamps.get_trim_index(successor.end(), strict=True, left=False)
                if idx == 0:
                    raise ValueError
                predecessor.timestamps = TimeSequence(predecessor.timestamps[: idx])  # Copy and trim

    def __str__(self):
        return f"{', '.join(map(str, self))}"

class Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next interval, if available


class Seed(NamedTuple):
    vertex: Vertex
    begin: Timestamp
    end: Timestamp
    candidates: Candidates
