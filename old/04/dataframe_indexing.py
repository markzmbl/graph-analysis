from line_profiler import profile
from datetime import datetime

from tqdm import tqdm
import pickle
import networkx as nx
import pandas as pd


@profile
def cycles():
    with open("20_278_946__2024-07-11.pickle", "rb") as graph_pickle:
        graph = pickle.load(graph_pickle)
    graph_df = nx.to_pandas_edgelist(graph)

    # Boolean Dataframe mask where source and target nodes are equal
    self_loop_mask = graph_df["source"] == graph_df["target"]
    # The actual values filtered by the mask
    self_loops = graph_df[self_loop_mask][["source", "target", "time"]].values

    for source, target, time in tqdm(self_loops, desc="Yielding self-loops"):
        yield [(source, target, time)]

    graph_df = graph_df[~self_loop_mask]

    multiplicities = {}
    for (source, target), group in tqdm(
            graph_df.sort_values(["source", "target", "time"]).groupby(["source", "target"]),
            desc="Precomputing multiplicities"
    ):
        multiplicity = group["time"].to_numpy()
        multiplicities[(source, target)] = multiplicity


    # Digons
    for u, v in tqdm(list(multiplicities)):
        digon_edges = graph_adjacency_df.loc[(v, u)]
        reverse_preceding_edges = digon_edges[digon_edges.index > time_start]
        if not reverse_preceding_edges.empty:
            for time_back in reverse_preceding_edges.index:
                yield [(u, v, time_start), (v, u, time_back)]


    histories = {}
    for source, group in tqdm(
            graph_df.sort_values(["source", "time"]).groupby("source"),
            desc="Precomputing histories"
    ):
        history = group[["time", "target"]].set_index("time")
        histories[source] = history

    #
    # for u, v, time_start in graph_adjacency_df.index.copy()[:30_000]:
    #     # Triangles
    #     try:
    #         traverse_edges = graph_adjacency_df.loc[v]
    #         traverse_predecing_edges = traverse_edges[traverse_edges.index.get_level_values("time") > time_start]
    #         if not traverse_predecing_edges.empty:
    #             for time_traverse, w in traverse_predecing_edges.index:
    #                 try:
    #                     reverse_edges = graph_adjacency_df.loc[(w, u)]
    #                     reverse_preceding_edges = reverse_edges[reverse_edges.index > time_traverse]
    #                     if not reverse_preceding_edges.empty:
    #                         for time_back in reverse_preceding_edges.index:
    #                             yield [(u, v, time_start), (v, w, time_traverse), (w, u, time_back)]
    #                 except KeyError:
    #                     pass
    #     except KeyError:
    #         pass


if __name__ == "__main__":
    list(cycles())
