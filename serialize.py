import json
from pathlib import Path
from typing import Callable, Sequence, Hashable

import numpy as np
import pandas as pd

from networkx.readwrite.json_graph.node_link import node_link_data, node_link_graph

from dscent.graph import TransactionGraph

_CACHE_DIRECTORY = Path("./cache")





class DataFrameSafeJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def decode_graph(x):
    if isinstance(x, str):
        data = json.loads(x)
        nx_graph = node_link_graph(data, edges="edges")
        return TransactionGraph.from_networkx(nx_graph)
    assert isinstance(x, TransactionGraph)
    return x  # Already a graph or None


def serialize_graph(graph: TransactionGraph) -> str:
    return json.dumps(node_link_data(graph.to_networkx(), edges="edges"), cls=DataFrameSafeJSONEncoder)

