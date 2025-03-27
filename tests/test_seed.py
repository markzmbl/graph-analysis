import pytest
from intervaltree import Interval

from dscent.iterator import _Candidates, _Seed


def test_candidates_initialization():
    """Test _Candidates set initialization and next_begin attribute."""
    candidates = _Candidates(["A", "B", "C"])
    assert isinstance(candidates, set)
    assert candidates == {"A", "B", "C"}

    # Test next_begin default value
    assert candidates.next_begin is None

    # Assign a value to next_begin and verify
    candidates.next_begin = 10
    assert candidates.next_begin == 10


def test_seed_initialization():
    """Test _Seed class initialization."""
    candidates = _Candidates(["X", "Y"])
    candidates.next_begin = 15
    interval = Interval(5, 10, candidates)

    seed = _Seed.construct("A", interval)

    assert seed.root == "A"
    assert seed.interval.begin == 5
    assert seed.interval.end == 10
    assert seed.candidates == {"A", "X", "Y"}  # Added Root Vertex
    assert seed.next_begin == 15


def test_seed_immutability():
    """Ensure that _Seed is immutable after creation."""
    candidates = _Candidates(["X", "Y"])
    interval = Interval(10, 30, candidates)
    candidates.next_begin = 20
    seed = _Seed.construct("A", interval)

    with pytest.raises(AttributeError):
        seed.root = "B"

    with pytest.raises(AttributeError):
        seed.begin = 0
