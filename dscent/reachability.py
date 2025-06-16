from __future__ import annotations

import heapq
from bisect import bisect_right, bisect_left
from collections import defaultdict
from collections.abc import Iterator
from copy import deepcopy
from typing import Iterable, Mapping, Literal

from sortedcontainers import SortedSet, SortedDict

from dscent.types_ import Vertex, Timestamp, PointVertex, PointVertices, SeriesVertex
from dscent.time_sequence import get_split_index, get_sequence_after, get_sequence_before

from collections import defaultdict
from sortedcontainers import SortedSet
from typing import Iterable

from collections import defaultdict
from sortedcontainers import SortedSet, SortedSet
from itertools import islice, chain
from bisect import bisect_left, bisect_right


class _TimestampsVerticesDict(SortedDict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getitem__(self, key):
        if key not in self:
            self[key] = set()
        return super().__getitem__(key)


class DirectReachability:
    def __init__(
            self, direct_reachability: DirectReachability | None = None,
            timestamp_vertices: Mapping[Timestamp, set[Vertex]] | Iterable[tuple[Timestamp, set[Vertex]]] = None
    ):
        if direct_reachability is not None:
            self._timestamps_vertices = _TimestampsVerticesDict(direct_reachability._timestamps_vertices)
        elif timestamp_vertices is not None:
            self._timestamps_vertices = _TimestampsVerticesDict(timestamp_vertices)
        else:
            self._timestamps_vertices: _TimestampsVerticesDict[Timestamp, set[Vertex]] = _TimestampsVerticesDict()

    def add(self, item: PointVertex | PointVertices):
        timestamp_vertices = self._timestamps_vertices[item.timestamp]
        try:
            timestamp_vertices.update(item.vertices)
        except AttributeError:
            timestamp_vertices.add(item.vertex)

    def empty(self):
        return len(self._timestamps_vertices) == 0

    def clear(self):
        self._timestamps_vertices.clear()

    def get_series_vertex(self, vertex: Vertex) -> SeriesVertex:
        timestamps = []
        for ts, vertices in self._timestamps_vertices.items():
            if vertex in vertices:
                timestamps.append(ts)
        return SeriesVertex(vertex=vertex, timestamps=timestamps)

    def exclude(self, vertex: Vertex):
        for ts, vertices in self._timestamps_vertices.items():
            vertices.discard(vertex)

    @property
    def timestamps(self) -> tuple[Timestamp, ...]:
        return tuple(self._timestamps_vertices.keys())

    def vertices(self) -> Iterable[Vertex]:
        yielded = set()
        for ts, vertices in self._timestamps_vertices.items():
            for vertex in vertices:
                if vertex not in yielded:
                    yield vertex
                    yielded.add(vertex)

    def merge(self, other: DirectReachability):
        for ts, vertices in other._timestamps_vertices.items():
            self._timestamps_vertices[ts].update(vertices)

    def __getitem__(self, item: Timestamp) -> set[Vertex]:
        return self._timestamps_vertices[item]

    def __delitem__(self, item: Timestamp):
        if item in self._timestamps_vertices:
            del self._timestamps_vertices[item]
        else:
            raise KeyError(f"Timestamp {item} not found in DirectReachability.")

    def __or__(self, other):
        result = DirectReachability(self)
        result.merge(other)
        return result

    def __ior__(self, other):
        self.merge(other)
        return self

    def __len__(self):
        return sum(len(v) for v in self._timestamps_vertices.values())

    @staticmethod
    def union(*args):
        result = DirectReachability()
        for arg in args:
            result.merge(arg)
        return result

    def _filter_by_time(
            self,
            limit: Timestamp,
            *,
            include_limit: bool,
            inplace: bool,
            direction: Literal["before", "after"],
    ) -> DirectReachability | None:
        index = get_split_index(
            self.timestamps,
            limit,
            direction=direction,
            include_limit=include_limit
        )
        if inplace:
            if direction == "before":
                to_remove = self.timestamps[index:]
            else:  # direction == "after"
                to_remove = self.timestamps[:index]
            for timestamp in to_remove:
                del self[timestamp]
            return None
        else:
            if direction == "before":
                to_keep = self.timestamps[:index]
            else:  # direction == "after"
                to_keep = self.timestamps[index:]
            return DirectReachability(timestamp_vertices={
                timestamp: self[timestamp] for timestamp in to_keep
            })

    def before(
            self,
            limit: Timestamp,
            *,
            include_limit: bool = False,
            inplace: bool = False,
    ) -> DirectReachability | None:
        return self._filter_by_time(limit, include_limit=include_limit, inplace=inplace, direction="before")

    def after(
            self,
            limit: Timestamp,
            *,
            include_limit: bool = False,
            inplace: bool = False,
    ) -> DirectReachability | None:
        return self._filter_by_time(limit, include_limit=include_limit, inplace=inplace, direction="after")


class SequentialReachability():
    def __init__(self, *args: SeriesVertex | Iterable[SeriesVertex]):
        if len(args) == 1 and isinstance(args[0], Iterable):
            self._series_vertices = list(args[0])
        else:
            self._series_vertices = list(args)

    def __len__(self) -> int:
        return len(self._series_vertices)

    def __iter__(self) -> Iterator[SeriesVertex]:
        return iter(self._series_vertices)

    def __getitem__(self, index: int) -> SeriesVertex | list[SeriesVertex]:
        if isinstance(index, slice):
            return self._series_vertices[index]
        return self._series_vertices[index]

    def __setitem__(self, index: int, value: SeriesVertex):
        if not isinstance(value, SeriesVertex):
            raise TypeError("Value must be a SeriesVertex instance.")
        self._series_vertices[index] = value

    def reverse_pairs(self) -> Iterator[tuple[SeriesVertex, SeriesVertex]]:
        return reversed(list(zip(self[:-1], self[1:])))

    def expand(self, item: SeriesVertex):
        if len(item.timestamps) == 0:
            raise ValueError

        if len(self) > 0:
            head = self[-1]
            if head.vertex == item.vertex:
                self[-1] = SeriesVertex(
                    vertex=head.vertex, timestamps=sorted(set(chain(head.timestamps, item.timestamps)))
                )
            else:
                if item.begin() <= head.begin():
                    # Copy and Trim
                    item = SeriesVertex(
                        vertex=item.vertex,
                        timestamps=tuple(get_sequence_after(item.timestamps, head.begin(), include_limit=False))
                    )
                    if len(item.timestamps) == 0:
                        raise ValueError
                self._series_vertices.append(item)

        for predecessor_index, (predecessor, successor) in zip(
                reversed(range(len(self) - 1)),
                self.reverse_pairs()
        ):
            successor_index = predecessor_index + 1
            assert self[predecessor_index].vertex == predecessor.vertex
            assert self[successor_index].vertex == successor.vertex
            # Check if predecessor timestamps need trimming
            if predecessor.end() >= successor.end():
                # Copy and trim
                predecessor = SeriesVertex(
                    vertex=predecessor.vertex,
                    timestamps=tuple(get_sequence_before(
                        predecessor.timestamps, successor.end(), include_limit=False
                    ))
                )
                if len(predecessor.timestamps) == 0:
                    raise ValueError
                self[predecessor_index] = predecessor
        assert len(set(v.vertex for v in self._series_vertices)) == len(
            list(v.vertex for v in self._series_vertices)), "Duplicate SeriesVertex detected."
