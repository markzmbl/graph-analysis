from __future__ import annotations

from collections import defaultdict
from collections.abc import Hashable, Sequence
from dataclasses import dataclass
from typing import Any, TypeVar

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


# class TimeSequenceABC(Sequence, ABC):


# class MutableTimeSequence(list, TimeSequenceABC):


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
    timestamps: Sequence[Timestamp]

    def begin(self) -> Timestamp:
        return self.timestamps[0]

    def end(self) -> Timestamp:
        return self.timestamps[-1]


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
