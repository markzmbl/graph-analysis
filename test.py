import itertools
from bisect import bisect_right, bisect_left
from collections import defaultdict
from collections.abc import Iterable
from typing import NamedTuple

import networkx as nx
import numpy as np
from matplotlib import pyplot as plt

from dscent.graph import TransactionGraph, ExplorationGraph
from dscent.types_ import Seed, Vertex, Timestamp, ReachabilitySet, TimedVertex

edges = [
    ("a", "b", 1),
    ("b", "d", 5),
    ("a", "b", 7),
    ("d", "a", 7),
    ("b", "d", 8),
    ("d", "e", 8),
    ("d", "f", 9),
    ("d", "a", 10),
    ("e", "c", 10),
    ("c", "d", 11),
    ("f", "a", 12),
    ("d", "b", 13),
]

transaction_graph = TransactionGraph()
for u, v, t in edges:
    transaction_graph.add_edge(u, v, key=t)
seed = Seed("a", 1, 13, transaction_graph.nodes)

exploration_graph = ExplorationGraph(
    transaction_graph
    .time_slice(seed.begin, seed.end, nodes=seed.candidates, closed=True)
)
exploration_graph.simple_cycles(seed.vertex, seed.candidates.next_begin)
