import pickle
import networkx as nx
from tqdm import tqdm
from utils import iteration_logging, get_drive
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

drive = get_drive()

graph_paths = sorted((p for p in (drive / "graphs").iterdir() if p.suffix == ".pickle"),
                     key=lambda x: x.stem.split("__")[-1])


def process_graph(graph_path, length_bound, log_directory):
    log_path = log_directory / f"{graph_path.stem}.csv"
    with graph_path.open("rb") as graph_file:
        graph = pickle.load(graph_file)

    for cycle in iteration_logging(nx.simple_cycles(graph, length_bound=length_bound), log_path):
        pass


def main():
    for length_bound in (3, 5, 7):
        log_directory = drive / f"simple_cycles/{length_bound}"
        log_directory.mkdir(parents=True, exist_ok=True)

        # Use a process pool for parallelization
        with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count() // 4) as executor:
            # Submit graph tasks for parallel processing
            futures = [
                executor.submit(process_graph, graph_path, length_bound, log_directory)
                for graph_path in graph_paths
            ]
            # Main progress bar for the outer loop (tracking graph files)
            for future in tqdm(futures, desc=f"Processing graphs with {length_bound=}", total=len(futures)):
                future.result()  # Wait for the graph processing to complete


if __name__ == "__main__":
    main()
