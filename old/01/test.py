# %%
from pathlib import Path
import pickle
from datetime import datetime, date


# %%
def parse_filename(path: Path) -> tuple[int, date]:
    block_number, graph_day = path.stem.split("__")
    graph_day = datetime.strptime(graph_day, "%Y-%m-%d").date()
    return (int(block_number), graph_day)


# %%
graphs_directory = Path("../16TB/graphs")

# %%
import numpy as np
import matplotlib.pyplot as plt

block_numbers = []
graphs_paths = list(graph_path for graph_path in graphs_directory.iterdir() if graph_path.suffix == ".pickle")
graphs_paths.sort()

for graph_path in graphs_paths:
    block_number, _ = parse_filename(graph_path)
    block_numbers.append(block_number)

# block_numbers.reverse()
plt.plot(np.diff(block_numbers))
plt.show()
# %%
import time
import networkx as nx
from datetime import timedelta


def iteration_logging(generator, log_path: Path, log_interval=30):
    """
    Tracks iterations over a generator and logs progress every log_interval seconds.

    Args:
        generator (iterable): The generator that yields the items to iterate over.
        log_interval (int): The current_time interval in seconds to log progress.
    """
    # Initialize timing details
    start_time = time.time()  # Record the starting current_time
    last_log_time = start_time  # Keep track of the last log current_time
    total_iterations = 0  # Total iterations done so far
    with log_path.open("w") as log_file:
        print("time_seconds,iterations,rate", file=log_file)
        for item in generator:
            total_iterations += 1
            current_time = time.time()

            # Check if the log interval has passed
            if current_time - last_log_time >= log_interval:
                elapsed_time = current_time - start_time
                rate = total_iterations / elapsed_time  # Iterations per second

                # Log progress: number of iterations and rate
                line = f"{elapsed_time},{total_iterations},{rate}"
                print(line, file=log_file)
                last_log_time = current_time  # Reset the last log current_time

            yield item  # Yield the item as usual for normal iteration flow

    # %%


import pickle
import networkx as nx

for graph_path in graphs_paths:
    logs_directory = graph_path.parents[1] / f"{graph_path.parent.stem}.logs"
    logs_directory.mkdir(parents=True, exist_ok=True)

    with graph_path.open("rb") as graph_pickle:
        graph = pickle.load(graph_pickle)

    log_path = logs_directory / f"{graph_path.stem}.csv"
    # cycles_generator = nx.chordless_cycles(graph)
    cycles_generator = range(100)
    for cycle in iteration_logging(cycles_generator, log_path):
        time.sleep(1)
    break
# %%
