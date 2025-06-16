# Define test vertices and timestamps
from pandas import Timestamp

from dscent.reachability import DirectReachability
from dscent.types_ import Vertex, PointVertex

ts1 = Timestamp("2024-01-01")
ts2 = Timestamp("2024-01-02")
ts3 = Timestamp("2024-01-03")
v1 = "v1"
v2 = "v2"
v3 = "v3"

# Create a unit test suite
def test_add_and_len():
    dr = DirectReachability()
    dr.add(PointVertex(timestamp=ts1, vertex=v1))
    dr.add(PointVertex(timestamp=ts2, vertex=v2))
    assert len(dr) == 2
    dr.add(PointVertex(timestamp=ts1, vertex=v3))
    assert len(dr) == 3

def test_get_series_vertex():
    dr = DirectReachability()
    dr.add(PointVertex(timestamp=ts1, vertex=v1))
    dr.add(PointVertex(timestamp=ts2, vertex=v1))
    series = dr.get_series_vertex(v1)
    assert series.vertex == v1
    assert series.timestamps == [ts1, ts2]

def test_exclude_vertex():
    dr = DirectReachability()
    dr.add(PointVertex(timestamp=ts1, vertex=v1))
    dr.add(PointVertex(timestamp=ts2, vertex=v2))
    dr.exclude(v1)
    assert v1 not in dr[ts1]
    assert v2 in dr[ts2]

def test_merge():
    dr1 = DirectReachability()
    dr1.add(PointVertex(timestamp=ts1, vertex=v1))
    dr2 = DirectReachability()
    dr2.add(PointVertex(timestamp=ts1, vertex=v2))
    dr2.add(PointVertex(timestamp=ts3, vertex=v3))
    dr1.merge(dr2)
    assert v1 in dr1[ts1]
    assert v2 in dr1[ts1]
    assert v3 in dr1[ts3]

def test_or_operator():
    dr1 = DirectReachability()
    dr1.add(PointVertex(timestamp=ts1, vertex=v1))
    dr2 = DirectReachability()
    dr2.add(PointVertex(timestamp=ts2, vertex=v2))
    dr3 = dr1 | dr2
    assert v1 in dr3[ts1]
    assert v2 in dr3[ts2]
    assert v1 not in dr2[ts1]

def test_ior_operator():
    dr1 = DirectReachability()
    dr1.add(PointVertex(timestamp=ts1, vertex=v1))
    dr2 = DirectReachability()
    dr2.add(PointVertex(timestamp=ts2, vertex=v2))
    dr1 |= dr2
    assert v1 in dr1[ts1]
    assert v2 in dr1[ts2]

def test_vertices_deduplication():
    dr = DirectReachability()
    dr.add(PointVertex(timestamp=ts1, vertex=v1))
    dr.add(PointVertex(timestamp=ts2, vertex=v1))
    all_vertices = list(dr.vertices())
    assert all_vertices == [v1]

def test_empty_and_clear():
    dr = DirectReachability()
    assert dr.empty()
    dr.add(PointVertex(timestamp=ts1, vertex=v1))
    assert not dr.empty()
    dr.clear()
    assert dr.empty()