from __future__ import annotations

from _bisect import bisect_right, bisect_left
from collections.abc import Iterable
from itertools import islice
from typing import Sequence, MutableSequence, Literal

from dscent.types_ import Timestamp


def get_split_index(
        sequence: Sequence[Timestamp],
        limit: Timestamp,
        *,
        direction: Literal["before", "after"] = "before",
        include_limit: bool = False
) -> int:
    assert all(sequence[i] <= sequence[i + 1] for i in range(len(sequence) - 1))
    if direction == "before":
        return bisect_right(sequence, limit) if include_limit else bisect_left(sequence, limit)
    elif direction == "after":
        return bisect_left(sequence, limit) if include_limit else bisect_right(sequence, limit)
    else:
        raise ValueError("Invalid value for 'direction'. Use 'before' or 'after'.")


def get_sequence_before(
        sequence: Sequence[Timestamp],
        limit: Timestamp,
        *,
        include_limit: bool = False
) -> tuple[Timestamp, ...]:
    index = get_split_index(sequence, limit, direction="before", include_limit=include_limit)
    return tuple(sequence[: index])


def get_sequence_after(
        sequence: Sequence[Timestamp],
        limit: Timestamp,
        *,
        include_limit: bool = False
) -> tuple[Timestamp, ...]:
    index = get_split_index(sequence, limit, direction="after", include_limit=include_limit)
    return tuple(sequence[index:])
