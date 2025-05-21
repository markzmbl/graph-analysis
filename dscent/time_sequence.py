from __future__ import annotations

from _bisect import bisect_right, bisect_left
from typing import Sequence, MutableSequence

from dscent.types_ import Timestamp


def get_split_index(sequence: Sequence[Timestamp], limit: Timestamp, strict: bool, left: bool) -> int:
    assert list(sequence) == sorted(sequence)

    if len(sequence) == 0:
        return 0
    elif left and limit <= sequence[0]:
        return 1 if strict else 0
    elif not left and limit >= sequence[-1]:
        return len(sequence) if strict else len(sequence) - 1

    if left:
        return bisect_right(sequence, limit) if strict else bisect_left(sequence, limit)
    else:
        return bisect_left(sequence, limit) if strict else bisect_right(sequence, limit)


def get_trimmed_before(
        time_sequence: Sequence[Timestamp],
        lower_limit: Timestamp, strict: bool = True
) -> Sequence[Timestamp]:
    idx = get_split_index(sequence=time_sequence, limit=lower_limit, strict=strict, left=True)
    return time_sequence[idx:]


def get_trimmed_after(
        time_sequence: Sequence[Timestamp],
        upper_limit: Timestamp, strict: bool = True
) -> Sequence[Timestamp]:
    idx = get_split_index(sequence=time_sequence, limit=upper_limit, strict=strict, left=False)
    return time_sequence[idx:]


def trim_before(
        time_sequence: MutableSequence[Timestamp],
        lower_limit: Timestamp, strict: bool = True
) -> None:
    idx = get_split_index(sequence=time_sequence, limit=lower_limit, strict=strict, left=True)
    del time_sequence[: idx]


def trim_after(
        time_sequence: MutableSequence[Timestamp],
        upper_limit: Timestamp, strict: bool = True
) -> None:
    idx = get_split_index(sequence=time_sequence, limit=upper_limit, strict=strict, left=False)
    del time_sequence[idx:]
