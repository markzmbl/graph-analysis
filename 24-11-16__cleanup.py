import pickle
import networkx as nx
from utils import get_graph_paths, read_graph
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
from functools import partial


def load_node_mapping(file_path):
    """
    Load node mapping from a file.
    """
    with open(file_path, "r") as f:
        return {bytes.fromhex(node_id_hex): i for i, node_id_hex in enumerate(f.readlines())}


def process_graph(graph_path, node_mapping):
    """
    Processes a single dynamic_graph file.
    """
    graph = read_graph(graph_path)
    edge_list = nx.to_pandas_edgelist(graph, edge_key="current_timestamp")

    # Apply mapping for source and target using the shared node_mapping
    edge_list["source_mapping"] = edge_list["source"].map(lambda x: node_mapping.get(x))
    edge_list["target_mapping"] = edge_list["target"].map(lambda x: node_mapping.get(x))

    edge_list.rename(
        inplace=True,
        columns={
            "id": "edge_id",
            "source": "source_id",
            "target": "target_id",
            "source_mapping": "source",
            "target_mapping": "target",
        },
    )
    graph = nx.from_pandas_edgelist(
        edge_list, create_using=nx.MultiDiGraph, edge_attr=["edge_id", "source_id", "target_id", "value"], edge_key="current_timestamp"
    )
    with graph_path.open("wb") as f:
        pickle.dump(graph, f)


def main():
    # Use a multiprocessing manager to create a shared dictionary
    manager = multiprocessing.Manager()
    node_mapping = manager.dict(load_node_mapping("node_ids.txt"))

    graph_paths = get_graph_paths()
    num_workers = multiprocessing.cpu_count() // 2

    # Create a partial function to fix the node_mapping argument
    process_graph_with_mapping = partial(process_graph, node_mapping=node_mapping)

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Map over the dynamic_graph paths only since node_mapping is fixed
        list(tqdm(executor.map(process_graph_with_mapping, graph_paths), total=len(graph_paths)))


if __name__ == "__main__":
    main()