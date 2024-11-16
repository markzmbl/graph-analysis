import pickle
import networkx as nx
from utils import get_graph_paths, read_graph
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

# Read node mapping
with open("node_ids.txt", "r") as f:
    node_mapping = {bytes.fromhex(node_id_hex): i for i, node_id_hex in enumerate(f.readlines())}


def process_graph(graph_path):
    """
    Processes a single graph file.
    """
    graph = read_graph(graph_path)
    edge_list = nx.to_pandas_edgelist(graph, edge_key="time")

    if not isinstance(edge_list["source"].dtype, int) or not isinstance(edge_list["target"].dtype, int):
        edge_list["source_mapping"] = edge_list["source"].map(node_mapping)
        edge_list["target_mapping"] = edge_list["target"].map(node_mapping)
        edge_list.rename(inplace=True, columns={
            "source": "source_id",
            "target": "target_id",
            "source_mapping": "source",
            "target_mapping": "target"
        })
        graph = nx.from_pandas_edgelist(edge_list, create_using=nx.MultiDiGraph, edge_attr=True, edge_key="time")
        with graph_path.open("wb") as f:
            pickle.dump(graph, f)


def main():
    graph_paths = get_graph_paths()
    # Set max_workers to the number of CPU cores
    num_workers = multiprocessing.cpu_count()

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Use list to ensure tqdm progress bar updates correctly
        list(tqdm(executor.map(process_graph, graph_paths), total=len(graph_paths)))


if __name__ == "__main__":
    main()