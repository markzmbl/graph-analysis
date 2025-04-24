from __future__ import annotations

from collections.abc import Iterator
from typing import Iterable

from dscent.types_ import Vertex, FrozenTimeSequence, SingleTimedVertex, Timestamp, MultiTimedVertex, TimeSequenceABC, \
    TimeSequence


class DirectReachability:
    vertices: list[Vertex]
    timestamps: TimeSequence

    # timestamps: TimeSequence

    def __init__(
            self,
            timed_vertices: Iterable[SingleTimedVertex] | DirectReachability | None = None,
            vertices: list[Vertex] | None = None,
            timestamps: list[Timestamp] | TimeSequenceABC | None = None,
    ):
        if isinstance(timed_vertices, DirectReachability):
            self.vertices = list(timed_vertices.vertices)
            self.timestamps = TimeSequence(timed_vertices.timestamps)
        else:
            self.vertices = vertices or []
            self.timestamps = timestamps or TimeSequence()
            for timed_vertex in timed_vertices or []:
                self.vertices.append(timed_vertex.vertex)
                self.timestamps.append(timed_vertex.timestamp)
        assert len(self.vertices) == len(self.timestamps)
        assert sorted(self.timestamps) == self.timestamps

    def get_trim_index(self, limit: Timestamp, strict: bool = True, left: bool = True) -> int:
        return self.timestamps.get_trim_index(limit=limit, strict=strict, left=left)

    def trim_before(self, lower_limit: Timestamp, strict=False) -> None:
        """
        Return the pruned  reachability set where stale entries are removed.

        :param lower_limit:
        :param strict: {(v, t) | t > upper_limit}
        """
        idx = self.get_trim_index(lower_limit, strict, left=True)
        del self[: idx]
        assert len(self.vertices) == len(self.timestamps)

    def get_trimmed_before(self, lower_limit: Timestamp, strict=False):
        idx = self.get_trim_index(lower_limit, strict, left=True)
        return self[idx:]

    def trim_after(self, upper_limit: Timestamp, strict=False) -> None:
        """
        Return the pruned reachability set where most recent entries are removed.

        :param upper_limit:
        :param strict: {(v, t) | t < upper_limit}
        """
        idx = self.get_trim_index(upper_limit, strict, left=False)
        del self[idx:]
        del self.vertices[idx:]
        assert len(self.vertices) == len(self.timestamps)

    def get_trimmed_after(self, upper_limit: Timestamp, strict=False):
        idx = self.get_trim_index(upper_limit, strict, left=False)
        return self[: idx]

    def append(self, item: SingleTimedVertex):
        """Appends a new root and its timestamp."""
        self.timestamps.append(item.timestamp)
        self.vertices.append(item.vertex)

    def extend(self, other: DirectReachability):
        """Extends the set with another DirectReachability."""
        self.timestamps.extend(other.timestamps)
        self.vertices.extend(other.vertices)

    def __len__(self):
        """Returns the number of stored elements."""
        return len(self.vertices)

    def __getitem__(self, index: int | slice) -> SingleTimedVertex | DirectReachability:
        """Allows indexed access to paired (root, timestamp) tuples."""
        vertices = self.vertices[index]
        timestamps = self.timestamps[index]
        if isinstance(index, slice):
            assert isinstance(vertices, list) and isinstance(timestamps, list)
            return DirectReachability(vertices=vertices, timestamps=timestamps)
        return SingleTimedVertex(vertices, timestamps)

    def __iter__(self) -> Iterable[SingleTimedVertex]:
        """Enables iteration, returning SingleTimedVertex objects."""
        return (
            SingleTimedVertex(vertex, timestamp)
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

    def __ior__(self, other: DirectReachability) -> DirectReachability:
        merged = self | other
        # Replace contents of self in-place
        self.clear()
        self.extend(merged)
        return self

    def __repr__(self) -> str:
        return repr(list(iter(self)))


class SequentialReachability(list[MultiTimedVertex]):
    def reverse_pairs(self) -> Iterator[tuple[MultiTimedVertex, MultiTimedVertex]]:
        return reversed(list(zip(self[:-1], self[1:])))

    def append(self, item: MultiTimedVertex):
        if len(self) > 0 and item.begin() < (head := self[-1]).begin():
            # Copy and Trim
            item = MultiTimedVertex(
                vertex=item.vertex,
                timestamps=item.timestamps.get_trimmed_before(head.begin(), strict=True)
            )

        if len(item.timestamps) == 0:
            raise ValueError

        super().append(item)
        for predecessor_index, (predecessor, successor) in enumerate(self.reverse_pairs()):
            # Check if predecessor timestamps need trimming
            if predecessor.end() > successor.end():
                # Copy and trim
                trimmed_time_sequence = predecessor.timestamps.get_trimmed_after(successor.end(), strict=True)
                self[predecessor_index] = MultiTimedVertex(
                    vertex=predecessor.vertex,
                    timestamps=trimmed_time_sequence
                )

    def __str__(self):
        return f"{', '.join(map(str, self))}"
