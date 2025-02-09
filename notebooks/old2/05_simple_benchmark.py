import pickle
import networkx as nx
from tqdm import tqdm
from utils import iteration_logging, get_drive
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

drive = get_drive()

graph_paths = sorted((p for p in (drive / "graphs").iterdir() if p.suffix == ".pickle"),
                     key=lambda x: x.stem.split("__")[-1])


def process_graph(graph_path, length_bound, log_directory, cycles_directory):
    log_path = log_directory / f"{graph_path.stem}.csv"
    cycles_path = cycles_directory / f"{graph_path.stem}.txt"
    with graph_path.open("rb") as graph_file:
        graph = pickle.load(graph_file)

    # Open the cycles file to log each cycle
    with cycles_path.open("w") as cycle_file:
        for cycle in iteration_logging(nx.simple_cycles(graph, length_bound=length_bound), log_path):
            cycle_file.write(f"{cycle}\n")  # Write each cycle to the file


def main():
    output_directory = drive / "simple_cycles"

    for length_bound in (3, 5, 7):
        length_bound_directory = output_directory / str(length_bound)
        log_directory = length_bound_directory / "logs"
        cycles_directory = length_bound_directory / "cycles"

        length_bound_directory.mkdir()
        log_directory.mkdir()
        cycles_directory.mkdir()

        # Use a process pool for parallelization
        with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count() // 4) as executor:
            # Submit transaction_graph tasks for parallel processing
            futures = [
                executor.submit(process_graph, graph_path, length_bound, log_directory, cycles_directory)
                for graph_path in graph_paths
            ]
            # Main progress bar for the outer loop (tracking transaction_graph files)
            for future in tqdm(futures, desc=f"Processing graphs with {length_bound=}", total=len(futures)):
                future.result()  # Wait for the transaction_graph processing to complete


if __name__ == "__main__":
    main()
