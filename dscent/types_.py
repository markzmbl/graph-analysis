from __future__ import annotations

from bisect import bisect_right, bisect_left
from abc import ABC, abstractmethod
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
    data: dict | None = None


class TimeSequence(list[Timestamp]):
    """A sorted sequence of timestamps with efficient trimming methods."""

    def __init__(self, timestamps: Iterable[Timestamp] | None = None):
        """
        Initializes a sorted sequence of timestamps.

        :param timestamps: A list of datetime objects.
        """
        if timestamps is None:
            timestamps = []
        assert list(timestamps) == sorted(timestamps), "Timestamps must be in sorted order"
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

    def _get_trim_index(self, limit: Timestamp, strict: bool, left: bool) -> int:
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

    def trim_before(self, lower_limit: Timestamp, strict: bool = True) -> None:
        """
        Removes timestamps before the given upper_limit.

        :param upper_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly before the upper_limit.
                       If False, removes elements before or equal to the upper_limit.
        :return: A new TimeSequence with the filtered timestamps.
        """
        idx = self._get_trim_index(lower_limit, strict, left=True)
        del self[: idx]

    def get_trimmed_before(self, lower_limit: Timestamp, strict: bool = True) -> TimeSequence:
        idx = self._get_trim_index(lower_limit, strict, left=True)
        return TimeSequence(self[idx:])

    def trim_after(self, upper_limit: Timestamp, strict: bool = True) -> None:
        """
        Removes timestamps after the given upper_limit.

        :param upper_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly after the upper_limit.
                       If False, removes elements after or equal to the upper_limit.
        :return: A new TimeSequence with the filtered timestamps.
        """
        idx = self._get_trim_index(upper_limit, strict, left=False)
        del self[idx:]

    def get_trimmed_after(self, upper_limit: Timestamp, strict: bool = True) -> TimeSequence:
        idx = self._get_trim_index(upper_limit, strict, left=False)
        return TimeSequence(self[: idx])


@dataclass
class TimedVertexABC(ABC):
    vertex: Vertex

    @abstractmethod
    def begin(self) -> Timestamp:
        """Returns the minimum timestamp."""
        raise NotImplementedError

    @abstractmethod
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
    data: list[dict] | None = None

    def begin(self) -> Timestamp:
        return self.timestamps.begin()

    def end(self):
        return self.timestamps.end()

    def __str__(self):
        return f"({self.vertex}, [{', '.join(map(str, self.timestamps))}])"


