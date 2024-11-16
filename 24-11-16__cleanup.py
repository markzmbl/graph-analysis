import pickle
import networkx as nx
from utils import get_graph_paths, read_graph
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
import multiprocessing


def load_node_mapping(file_path):
    """
    Load node mapping from a file.
    """
    with open(file_path, "r") as f:
        return {bytes.fromhex(node_id_hex): i for i, node_id_hex in enumerate(f.readlines())}


def process_graph(graph_path, node_mapping):
    """
    Processes a single graph file.
    """
    graph = read_graph(graph_path)
    print("read graph")
    edge_list = nx.to_pandas_edgelist(graph, edge_key="time")
    print("generated edge list")

    # Apply mapping for source and target using the read-only node_mapping
    edge_list["source_mapping"] = edge_list["source"].map(node_mapping)
    print("mapped source")
    edge_list["target_mapping"] = edge_list["target"].map(node_mapping)
    print("mapped target")

    edge_list.rename(
        inplace=True,
        columns={
            "source": "source_id",
            "target": "target_id",
            "source_mapping": "source",
            "target_mapping": "target",
        },
    )
    graph = nx.from_pandas_edgelist(
        edge_list, create_using=nx.MultiDiGraph, edge_attr=True, edge_key="time"
    )
    print("created new graph")
    with graph_path.open("wb") as f:
        pickle.dump(graph, f)
        print("saved graph")


def main():
    # Load the read-only node mapping once
    node_mapping = load_node_mapping("node_ids.txt")

    graph_paths = get_graph_paths()
    # Set max_workers to the number of CPU cores
    num_workers = multiprocessing.cpu_count() // 2

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Use partial function application to pass node_mapping as a fixed argument
        list(tqdm(executor.map(lambda gp: process_graph(gp, node_mapping), graph_paths), total=len(graph_paths)))


if __name__ == "__main__":
    main()