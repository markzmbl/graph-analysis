from __future__ import annotations

from abc import ABC
from bisect import bisect_right, bisect_left
from collections import defaultdict
from collections.abc import Iterable, Sequence, Hashable
from dataclasses import dataclass, field
from typing import NamedTuple, Any, TypeVar, Generic

from sortedcontainers import SortedList

# --- Type variables
Vertex = TypeVar("Vertex", bound=Hashable)  # Vertex
Timestamp = TypeVar("Timestamp", bound=int)  # Timestamp
Timedelta = TypeVar("Timedelta", bound=int)  # Timedelta


# --- Core structures
@dataclass(frozen=True)
class Interval:
    begin: Timestamp
    end: Timestamp


@dataclass(frozen=True)
class Interaction:
    target: Vertex
    timestamp: Timestamp


@dataclass(frozen=True)
class EdgeInteraction(Interaction):
    source: Vertex


@dataclass(frozen=True)
class TargetInteraction(Interaction):
    sources: list[Vertex]


class TransactionBlock(defaultdict[Vertex, list[Vertex]]):
    def __init__(self, timestamp: Timestamp, **kwargs: Any):
        super().__init__(list, **kwargs)
        self.timestamp = timestamp


# --- Time Sequence Base Class

class TimeSequence(Sequence[Timestamp], ABC):
    # def __init__(self, *args: Any, **kwargs: Any):
    #     super().__init__(*args, **kwargs)
    #     assert self == sorted(self)

    def begin(self) -> Timestamp:
        if len(self) == 0:
            raise ValueError(f"{self.__class__.__name__} is empty, cannot retrieve the first element.")
        return self[0]

    def end(self) -> Timestamp:
        if len(self) == 0:
            raise ValueError(f"{self.__class__.__name__} is empty, cannot retrieve the last element.")
        return self[-1]

    def get_split_index(self, limit: Timestamp, strict: bool, left: bool) -> int:
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

    def get_trimmed_before(self, lower_limit: Timestamp, strict: bool = True) -> TimeSequence:
        idx = self.get_split_index(lower_limit, strict, left=True)
        return self[idx:]

    def get_trimmed_after(self, upper_limit: Timestamp, strict: bool = True) -> TimeSequence:
        idx = self.get_split_index(upper_limit, strict, left=False)
        return self[:idx]

    def __getitem__(self, index: int | slice) -> Timestamp | TimeSequence[Timestamp]:
        result = super().__getitem__(index)
        if isinstance(index, slice):
            return self.__class__(result)
        return result


class FrozenTimeSequence(tuple, TimeSequence):
    """A sorted, immutable sequence of timestamps."""
    pass


class MutableTimeSequence(list, TimeSequence):
    """A sorted, mutable sequence of timestamps."""

    def trim_before(self, lower_limit: Timestamp, strict: bool = True) -> None:
        idx = self.get_split_index(lower_limit, strict, left=True)
        del self[:idx]

    def trim_after(self, upper_limit: Timestamp, strict: bool = True) -> None:
        idx = self.get_split_index(upper_limit, strict, left=False)
        del self[idx:]


# --- TimedEvent Vertices

class TimedEvent:
    def begin(self) -> Timestamp:
        raise NotImplementedError

    def end(self) -> Timestamp:
        raise NotImplementedError


@dataclass(frozen=True)
class InstantaneousEvent(TimedEvent):
    """A point in time associated with a vertex."""
    timestamp: Timestamp

    def begin(self) -> Timestamp:
        return self.timestamp

    def end(self) -> Timestamp:
        return self.timestamp


@dataclass(frozen=True)
class TemporalSpanEvent(TimedEvent):
    """A point in time associated with a vertex."""
    timestamps: FrozenTimeSequence

    def begin(self) -> Timestamp:
        return self.timestamps.begin()

    def end(self) -> Timestamp:
        return self.timestamps.end()


@dataclass(frozen=True)
class PointVertex(InstantaneousEvent):
    """A vertex associated with a single point in time."""
    vertex: Vertex

    def __lt__(self, other: PointVertex) -> bool:
        return (self.begin(), self.vertex) < (other.begin(), other.vertex)


@dataclass(frozen=True)
class PointVertices(InstantaneousEvent):
    """A collection of vertices associated with a single point in time."""
    vertices: list[Vertex]


@dataclass(frozen=True)
class SeriesVertex(TemporalSpanEvent):
    """A vertex associated with multiple timestamps."""
    vertex: Vertex
