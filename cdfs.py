import itertools
from bisect import bisect_right, bisect_left
from collections import defaultdict
from collections.abc import Iterable
from typing import NamedTuple

import networkx as nx
import numpy as np
from matplotlib import pyplot as plt

from dscent.graph import TransactionGraph, ExplorationGraph
from dscent.seed import Candidates, Seed
from dscent.types_ import Interval
from measure import get_path_bundle

edges = [
    ("a", "c", 5),
    ("a", "b", 1),
    ("b", "c", 5),
    ("c", "d", 6),
    ("c", "e", 7),
    ("b", "c", 8),
    ("d", "a", 8),
    ("b", "c", 10),
    ("e", "f", 10),
    ("c", "h", 11),
    ("f", "a", 12),
    ("h", "j", 13),
    ("h", "k", 14),
    ("k", "j", 15),
    ("j", "b", 16),
    ("b", "a", 17),
]

transaction_graph = TransactionGraph()
for u, v, t in edges:
    transaction_graph.add_edge(u, v, timestamp=t)
candidates = Candidates(transaction_graph.nodes)
candidates.next_begin = 17
seed = Seed("a", Interval(1, 17), candidates, None)

exploration_graph = ExplorationGraph(
    transaction_graph
    .time_slice(seed.interval.begin, seed.interval.end, nodes=seed.candidates, closed=True),
    root_vertex=seed.root
)
for c in exploration_graph.simple_cycles(seed.candidates.next_begin):
    print(get_path_bundle(TransactionGraph.from_networkx(c)))

