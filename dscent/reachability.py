from __future__ import annotations

import heapq
from collections.abc import Iterator
from typing import Iterable

from dscent.types_ import Vertex, Timestamp, PointVertex, PointVertices, SeriesVertex
from dscent.time_sequence import get_split_index, get_trimmed_after, get_trimmed_before


class DirectReachability:
    vertices: list[Vertex]
    timestamps: list[Timestamp]

    def __init__(
            self,
            timed_vertices: Iterable[PointVertex[Vertex]] | DirectReachability | None = None,
            vertices: list[Vertex] | None = None,
            timestamps: list[Timestamp] | None = None,
    ):
        if isinstance(timed_vertices, DirectReachability):
            self.vertices = list(timed_vertices.vertices)
            self.timestamps = list(timed_vertices.timestamps)
        else:
            self.vertices = vertices or []
            self.timestamps = timestamps or []
            for timed_vertex in timed_vertices or []:
                self.vertices.append(timed_vertex.vertex)
                self.timestamps.append(timed_vertex.timestamp)
        assert len(self.vertices) == len(self.timestamps)
        assert sorted(self.timestamps) == self.timestamps

    def trim_before(self, lower_limit: Timestamp, strict=False) -> None:
        """
        Return the pruned  reachability set where stale entries are removed.

        :param lower_limit:
        :param strict: {(v, t) | t >= upper_limit}
        """
        idx = get_split_index(self.timestamps, lower_limit, strict, left=True)
        del self[: idx]
        assert len(self.vertices) == len(self.timestamps)

    def get_trimmed_before(self, lower_limit: Timestamp, strict=False):
        idx = get_split_index(self.timestamps, lower_limit, strict, left=True)
        return self[idx:]

    def trim_after(self, upper_limit: Timestamp, strict=False) -> None:
        """
        Return the pruned reachability set where most recent entries are removed.

        :param upper_limit:
        :param strict: {(v, t) | t <= upper_limit}
        """
        idx = get_split_index(self.timestamps, upper_limit, strict, left=False)
        del self[idx:]
        assert len(self.vertices) == len(self.timestamps)

    def get_trimmed_after(self, upper_limit: Timestamp, strict=False):
        idx = get_split_index(self.timestamps, upper_limit, strict, left=False)
        return self[: idx]

    def add(self, item: PointVertex | PointVertices) -> None:
        """Adds a new vertex and its timestamp in sorted order."""
        vertices = [item.vertex] if isinstance(item, PointVertex) else item.vertices
        idx = get_split_index(self.timestamps, limit=item.timestamp, strict=True, left=False)
        self.timestamps[idx: idx] = len(vertices) * [item.timestamp]
        self.vertices[idx: idx] = vertices

    def append(self, item: PointVertex | PointVertices) -> None:
        """Adds a new vertex and its timestamp at the end."""
        vertices = [item.vertex] if isinstance(item, PointVertex) else item.vertices
        for vertex in vertices:
            self.timestamps.append(item.timestamp)
            self.vertices.append(vertex)

    def extend(self, other: DirectReachability) -> None:
        """Extends the set with another DirectReachability."""
        self.timestamps.extend(other.timestamps)
        self.vertices.extend(other.vertices)

    def __len__(self) -> int:
        """Returns the number of stored elements."""
        return len(self.vertices)

    def __getitem__(self, index: int | slice) -> PointVertex[Vertex, Timestamp] | DirectReachability:
        """Allows indexed access to paired (root, timestamp) tuples."""
        vert = self.vertices[index]
        time = self.timestamps[index]
        if isinstance(index, slice):
            assert isinstance(vert, list) and isinstance(time, list)
            return DirectReachability(vertices=vert, timestamps=time)
        return PointVertex(vertex=vert, timestamp=time)

    def __iter__(self) -> Iterable[PointVertex[Vertex, Timestamp]]:
        """Enables iteration, returning SingleTimedVertex objects."""
        return (
            PointVertex(vertex=vertex, timestamp=timestamp)
            for vertex, timestamp
            in zip(self.vertices, self.timestamps)
        )

    def clear(self) -> None:
        self.vertices.clear()
        self.timestamps.clear()

    def __delitem__(self, index: int | slice):
        """Deletes elements by index or slice."""
        del self.vertices[index]
        del self.timestamps[index]

    def __or__(self, other: DirectReachability) -> DirectReachability:
        """Merge two sorted lists of unique tuples while preserving order and uniqueness."""
        i, j = 0, 0
        merged = DirectReachability()

        while i < len(self) and j < len(other):
            if self[i] < other[j]:
                merged.add(self[i])
                i += 1
            elif other[j] < self[i]:
                merged.add(other[j])
                j += 1
            else:
                # They are equal, add only one copy
                merged.add(self[i])
                i += 1
                j += 1

        # Append remaining elements (if any)
        merged.extend(self[i:])
        merged.extend(other[j:])

        return merged

    def __ior__(self, other: DirectReachability) -> DirectReachability:
        merged = self | other
        # Replace contents of self in-place
        self.clear()
        self.extend(merged)
        return self

    @staticmethod
    def union(*args: DirectReachability) -> DirectReachability:
        """Perform union like set.union: can be called on instance or class."""
        import heapq

        if not args:
            return DirectReachability()

        result = DirectReachability()
        heap = []
        last_added = None

        # Initialize the heap with the first element of each input, along with list and index info
        for list_index, direct_reachability in enumerate(args):
            if direct_reachability:
                heapq.heappush(heap, (direct_reachability[0], list_index, 0))

        while heap:
            point_vertex, list_index, item_index = heapq.heappop(heap)

            if point_vertex != last_added:
                result.add(point_vertex)
                last_added = point_vertex

            # Push the next element from the same input list
            next_index = item_index + 1
            if next_index < len(args[list_index]):
                heapq.heappush(heap, (args[list_index][next_index], list_index, next_index))

        return result

    def __repr__(self) -> str:
        return repr(list(iter(self)))


class SequentialReachability(list[SeriesVertex]):
    def reverse_pairs(self) -> Iterator[tuple[SeriesVertex, SeriesVertex]]:
        return reversed(list(zip(self[:-1], self[1:])))

    def append(self, item: SeriesVertex):
        if len(self) > 0 and item.begin() < (head := self[-1]).begin():
            # Copy and Trim
            item = SeriesVertex(
                vertex=item.vertex,
                timestamps=get_trimmed_before(item.timestamps, head.begin(), strict=True)
            )

        if len(item.timestamps) == 0:
            raise ValueError

        super().append(item)
        for predecessor_index, (predecessor, successor) in enumerate(self.reverse_pairs()):
            # Check if predecessor timestamps need trimming
            if predecessor.end() > successor.end():
                # Copy and trim
                trimmed_time_sequence = get_trimmed_after(predecessor.timestamps, successor.end(), strict=True)
                self[predecessor_index] = SeriesVertex(
                    vertex=predecessor.vertex,
                    timestamps=trimmed_time_sequence
                )
