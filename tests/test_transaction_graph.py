import pytest
import networkx as nx
from dscent.types_ import Timestamp, TimeDelta, Vertex
from dscent.graph import TransactionGraph  # Adjust if needed


def test_transaction_graph_time_slice():
    # Create a TransactionGraph
    g = TransactionGraph()

    # Define timestamps
    t1, t2, t3 = 1, 5, 10  # Sample timestamps

    # Add edges (u, v, timestamp)
    g.add_edge("A", "B", key=t1)
    g.add_edge("B", "C", key=t2)
    g.add_edge("C", "D", key=t3)

    # **Closed Interval [1, 5]**
    subgraph_closed = g.time_slice(begin=1, end=5, closed=True)
    assert len(subgraph_closed.edges) == 2, "time_slice(begin=1, upper_limit=5, closed=True) should include t1 and t2"

    # **Half-Open Interval [1, 5)**
    subgraph_open = g.time_slice(begin=1, end=5, closed=False)
    assert len(subgraph_open.edges) == 1, "time_slice(begin=1, upper_limit=5, closed=False) should include only t1 (exclude t2)"

    # **Test full range, closed**
    subgraph_full_closed = g.time_slice(begin=1, end=10, closed=True)
    assert len(subgraph_full_closed.edges) == 3, "time_slice(begin=1, upper_limit=10, closed=True) should include all edges"

    # **Test full range, open at upper_limit**
    subgraph_full_open_end = g.time_slice(begin=1, end=10, closed=False)
    assert len(subgraph_full_open_end.edges) == 2, "time_slice(begin=1, upper_limit=10, closed=False) should exclude t3"