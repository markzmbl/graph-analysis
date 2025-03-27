import pickle
import pytest
import networkx as nx

from dscent.graph import ExplorationGraph, _ClosureManager

@pytest.fixture
def sample_graph():
    """Create a small sample ExplorationGraph with a few nodes and edges."""
    graph = ExplorationGraph()
    graph.add_edge("A", "B", key=1)
    graph.add_edge("B", "C", key=2)
    graph.add_edge("C", "D", key=3)
    graph.graph["root_vertex"] = "A"
    return graph

def test_exploration_graph_pickle(sample_graph):
    """Ensure that ExplorationGraph can be pickled and unpickled."""
    pickled_data = pickle.dumps(sample_graph)  # Serialize
    unpickled_graph = pickle.loads(pickled_data)  # Deserialize

    # Check if graph structure remains the same
    assert set(sample_graph.nodes) == set(unpickled_graph.nodes)
    assert set(sample_graph.edges(keys=True)) == set(unpickled_graph.edges(keys=True))

    # Ensure root_vertex is preserved
    assert unpickled_graph.graph["root_vertex"] == sample_graph.graph["root_vertex"]

    # Ensure closure is restored properly
    assert isinstance(unpickled_graph.closure, _ClosureManager)
    assert len(unpickled_graph.closure) == len(sample_graph.closure)