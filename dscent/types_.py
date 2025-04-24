from __future__ import annotations

from bisect import bisect_right, bisect_left
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Hashable, NamedTuple, Sequence

Vertex = Hashable
Timestamp = int
TimeDelta = int


class Interval(NamedTuple):
    begin: Timestamp
    end: Timestamp


class Interaction(NamedTuple):
    source: Vertex
    target: Vertex
    timestamp: Timestamp


class TimeSequenceABC(Sequence[Timestamp], ABC):
    def __init__(self, timestamps: Sequence[Timestamp] | None):
        if timestamps is not None:
            assert list(timestamps) == sorted(timestamps), "Timestamps must be in sorted order"

    def begin(self) -> Timestamp:
        """
        Returns the first timestamp in the sequence.
        Raises a ValueError if the sequence is empty.
        """
        try:
            return next(iter(self))
        except StopIteration:
            raise ValueError("FrozenTimeSequence is empty, cannot retrieve the first element.")

    def end(self) -> Timestamp:
        """
        Returns the last timestamp in the sequence.
        Raises a ValueError if the sequence is empty.
        """
        try:
            return next(reversed(self))
        except StopIteration:
            raise ValueError("FrozenTimeSequence is empty, cannot retrieve the last element.")

    def get_trim_index(self, limit: Timestamp, strict: bool, left: bool) -> int:
        """
        Returns the appropriate index for trimming operations using bisect.

        :param limit: The threshold timestamp.
        :param strict: Determines whether to exclude the limit itself.
        :param left: If True, trims from the left (before limit). Otherwise, trims from the right (after limit).
        :return: The index to slice the list.
        """
        if len(self) == 0:
            return 0
        elif left and limit <= self[0]:
            return 1 if strict else 0
        elif not left and limit >= self[-1]:
            return len(self) if strict else len(self) - 1

        if left:
            return bisect_right(self, limit) if strict else bisect_left(self, limit)
        else:
            return bisect_left(self, limit) if strict else bisect_right(self, limit)


class FrozenTimeSequence(tuple[Timestamp], TimeSequenceABC):
    """A sorted sequence of timestamps with efficient trimming methods."""

    def __init__(self, timestamps: Iterable[Timestamp] | None = None):
        """
        Initializes a sorted sequence of timestamps.

        :param timestamps: A list of datetime objects.
        """
        super().__init__(tuple(timestamps or []))

    def get_trimmed_before(self, lower_limit: Timestamp, strict: bool = True) -> FrozenTimeSequence:
        idx = self.get_trim_index(lower_limit, strict, left=True)
        return FrozenTimeSequence(self[idx:])

    def get_trimmed_after(self, upper_limit: Timestamp, strict: bool = True) -> FrozenTimeSequence:
        idx = self.get_trim_index(upper_limit, strict, left=False)
        return FrozenTimeSequence(self[: idx])


class TimeSequence(list[Timestamp], TimeSequenceABC):
    def __init__(self, timestamps: Sequence[Timestamp] | None = None):
        """
        Initializes a sorted, mutable sequence of timestamps.

        :param timestamps: A list of datetime objects.
        """
        super().__init__(list(timestamps or []))

    def trim_before(self, lower_limit: Timestamp, strict: bool = True) -> None:
        """
        Removes timestamps before the given upper_limit.

        :param lower_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly before the upper_limit.
                       If False, removes elements before or equal to the upper_limit.
        :return: A new FrozenTimeSequence with the filtered timestamps.
        """
        idx = self.get_trim_index(lower_limit, strict, left=True)
        del self[: idx]

    def trim_after(self, upper_limit: Timestamp, strict: bool = True) -> None:
        """
        Removes timestamps after the given upper_limit.

        :param upper_limit: The threshold timestamp.
        :param strict: If True, removes elements strictly after the upper_limit.
                       If False, removes elements after or equal to the upper_limit.
        :return: A new FrozenTimeSequence with the filtered timestamps.
        """
        idx = self.get_trim_index(upper_limit, strict, left=False)
        del self[idx:]


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class SingleTimedVertex(TimedVertexABC):
    timestamp: Timestamp

    def begin(self) -> Timestamp:
        return self.timestamp

    def end(self):
        return self.timestamp

    def __str__(self):
        return f"({self.vertex}, {repr(self.timestamp)})"


@dataclass(frozen=True)
class MultiTimedVertex(TimedVertexABC):
    timestamps: FrozenTimeSequence

    def begin(self) -> Timestamp:
        return self.timestamps.begin()

    def end(self):
        return self.timestamps.end()

    def __str__(self):
        return f"({self.vertex}, [{', '.join(map(str, self.timestamps))}])"
