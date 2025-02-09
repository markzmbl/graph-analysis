import pickle

import networkx as nx
from algorithms import simple_temporal_cycles
from tqdm.notebook import tqdm

from utils import iteration_logging, get_drive

with open("../16TB/graphs/10_002_872__2020-05-05.pickle", "rb") as graph_pickle:
   graph: nx.MultiDiGraph = pickle.load(graph_pickle)


edges = nx.to_pandas_edgelist(graph)
edges = edges.sort_values(["current_timestamp", "source", "target"])
graph = nx.from_pandas_edgelist(edges, create_using=nx.MultiDiGraph, edge_attr=True, edge_key="current_timestamp")

for length_bound in range(1, 8):
    for cycle in simple_temporal_cycles(graph, length_bound=length_bound):
        pass