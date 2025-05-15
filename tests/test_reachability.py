import pytest

from dscent.reachability import DirectReachability, SequentialReachability
from dscent.types_ import SingleTimedVertex, FrozenTimeSequence, MultiTimedVertex


def test_direct_reachability_initialization():
    """Test initialization with different inputs."""
    dr_empty = DirectReachability()
    assert len(dr_empty) == 0

    vertices = ["A", "B", "C"]
    timestamps = [10, 20, 30]
    dr = DirectReachability(vertices=vertices, timestamps=timestamps)

    assert dr.vertices == vertices
    assert dr.timestamps == timestamps

    # Test initialization with SingleTimedVertex instances
    timed_vertices = [SingleTimedVertex("X", 5), SingleTimedVertex("Y", 15)]
    dr_tv = DirectReachability(timed_vertices=timed_vertices)
    assert dr_tv.vertices == ["X", "Y"]
    assert dr_tv.timestamps == [5, 15]

    # Test initialization from another DirectReachability
    dr_copy = DirectReachability(dr)
    assert dr_copy.vertices == vertices
    assert dr_copy.timestamps == timestamps

    # Test that non-sorted timestamps raise an assertion error
    with pytest.raises(AssertionError, match="Timestamps must be in sorted order"):
        DirectReachability(vertices=["A", "B"], timestamps=[20, 10])


def test_get_trim_index():
    """Test get_split_index method."""
    dr = DirectReachability(vertices=["A", "B", "C"], timestamps=[10, 20, 30])

    assert dr.get_split_index(15, strict=True, left=True) == 1
    assert dr.get_split_index(20, strict=False, left=True) == 1
    assert dr.get_split_index(25, strict=True, left=False) == 2
    assert dr.get_split_index(30, strict=False, left=False) == 3


def test_trim_before():
    """Test trim_before method."""
    dr = DirectReachability(vertices=["A", "B", "C", "D"], timestamps=[10, 20, 30, 40])

    dr.trim_before(25, strict=True)
    assert dr.vertices == ["C", "D"]
    assert dr.timestamps == [30, 40]

    dr = DirectReachability(vertices=["A", "B", "C", "D"], timestamps=[10, 20, 30, 40])
    dr.trim_before(20, strict=False)
    assert dr.vertices == ["B", "C", "D"]
    assert dr.timestamps == [20, 30, 40]


def test_trim_after():
    """Test trim_after method."""
    dr = DirectReachability(vertices=["A", "B", "C", "D"], timestamps=[10, 20, 30, 40])

    dr.trim_after(25, strict=True)
    assert dr.vertices == ["A", "B"]
    assert dr.timestamps == [10, 20]

    dr = DirectReachability(vertices=["A", "B", "C", "D"], timestamps=[10, 20, 30, 40])
    dr.trim_after(30, strict=False)
    assert dr.vertices == ["A", "B", "C"]
    assert dr.timestamps == [10, 20, 30]


def test_append():
    """Test append method."""
    dr = DirectReachability()
    vertex = SingleTimedVertex("A", 100)

    dr.append(vertex)
    assert dr.vertices == ["A"]
    assert dr.timestamps == [100]


def test_extend():
    """Test extend method."""
    dr1 = DirectReachability(vertices=["A", "B"], timestamps=[10, 20])
    dr2 = DirectReachability(vertices=["C", "D"], timestamps=[30, 40])

    dr1.extend(dr2)
    assert dr1.vertices == ["A", "B", "C", "D"]
    assert dr1.timestamps == [10, 20, 30, 40]


def test_getitem():
    """Test __getitem__ method."""
    dr = DirectReachability(vertices=["A", "B", "C"], timestamps=[10, 20, 30])

    assert dr[0].vertex == "A"
    assert dr[0].timestamp == 10

    dr_slice = dr[1:]
    assert isinstance(dr_slice, DirectReachability)
    assert dr_slice.vertices == ["B", "C"]
    assert dr_slice.timestamps == [20, 30]


def test_iter():
    """Test __iter__ method."""
    dr = DirectReachability(vertices=["A", "B"], timestamps=[10, 20])
    vertices = list(iter(dr))

    assert isinstance(vertices[0], SingleTimedVertex)
    assert vertices[0].vertex == "A"
    assert vertices[0].timestamp == 10
    assert vertices[1].vertex == "B"
    assert vertices[1].timestamp == 20


def test_delitem():
    """Test __delitem__ method."""
    dr = DirectReachability(vertices=["A", "B", "C"], timestamps=[10, 20, 30])

    del dr[1]
    assert dr.vertices == ["A", "C"]
    assert dr.timestamps == [10, 30]


def test_union_operator():
    """Test __or__ method (union)."""
    dr1 = DirectReachability(vertices=["A", "B"], timestamps=[10, 20])
    dr2 = DirectReachability(vertices=["B", "C"], timestamps=[20, 30])

    dr_union = dr1 | dr2
    assert dr_union.vertices == ["A", "B", "C"]
    assert dr_union.timestamps == [10, 20, 30]


@pytest.fixture
def sample_vertices():
    timestamps1 = FrozenTimeSequence([1, 2, 3, 4])
    timestamps2 = FrozenTimeSequence([3, 4, 5, 6])
    timestamps3 = FrozenTimeSequence([6, 7, 8])

    v1 = MultiTimedVertex("A", timestamps=timestamps1)
    v2 = MultiTimedVertex("B", timestamps=timestamps2)
    v3 = MultiTimedVertex("C", timestamps=timestamps3)

    return v1, v2, v3


def test_append_valid_sequence(sample_vertices):
    seq = SequentialReachability()
    v1, v2, v3 = sample_vertices

    seq.append(v1)
    seq.append(v2)
    seq.append(v3)

    assert len(seq) == 3
    assert seq[0] == v1
    assert seq[1] == v2
    assert seq[2] == v3


def test_append_with_trimming(sample_vertices):
    seq = SequentialReachability()
    v1, v2, _ = sample_vertices

    seq.append(v1)
    seq.append(v2)

    assert v2.timestamps == FrozenTimeSequence([3, 4, 5, 6])  # Should be trimmed based on logic


def test_append_invalid_timestamps():
    seq = SequentialReachability()

    timestamps1 = FrozenTimeSequence([5, 6, 7])
    timestamps2 = FrozenTimeSequence([1, 2, 3])  # This is inconsistent as it starts before

    v1 = MultiTimedVertex("A", timestamps=timestamps1)
    v2 = MultiTimedVertex("B", timestamps=timestamps2)

    seq.append(v1)
    with pytest.raises(ValueError):
        seq.append(v2)  # Should raise ValueError due to trimming logic


def test_predecessor_trimming():
    seq = SequentialReachability()

    timestamps1 = FrozenTimeSequence([1, 2, 3, 4, 5])
    timestamps2 = FrozenTimeSequence([3, 4])  # Shorter sequence

    v1 = MultiTimedVertex("A", timestamps=timestamps1)
    v2 = MultiTimedVertex("V", timestamps=timestamps2)

    seq.append(v1)
    seq.append(v2)

    assert (
            v1.timestamps
            != seq[0].timestamps  # Check if copy
            == FrozenTimeSequence([1, 2, 3])  # Should be trimmed
    )
    assert v2.timestamps == FrozenTimeSequence([3, 4])


def test_str_representation(sample_vertices):
    seq = SequentialReachability()
    v1, v2, v3 = sample_vertices

    seq.append(v1)
    seq.append(v2)
    seq.append(v3)

    assert str(seq) == f"{v1}, {v2}, {v3}"
