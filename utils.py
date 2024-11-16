import argparse
import pickle
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, Future
from datetime import datetime, date

import networkx as nx
import numpy as np
import psutil
from pathlib import Path
from typing import Generator, Iterable, Any

DATE_FORMAT = "%Y-%m-%d"


def parse_date(date_str):
    """Parse a date string in the format YYYY-MM-DD."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD.")


def read_graph(graph_path: Path) -> nx.MultiDiGraph:
    with open(graph_path, "rb") as f:
        return pickle.load(f)


def parse_filename(graph_path: Path) -> tuple[int, date]:
    block_number, graph_date, *_ = graph_path.stem.split("__")
    graph_date = parse_date(graph_date)
    return int(block_number), graph_date


def get_graph_paths(
        start_date: date | str | None = None, end_date: date | str | None = None) -> list[Path]:
    # Convert start_date and end_date to `date` objects if necessary
    if start_date is not None and not isinstance(start_date, date):
        start_date = parse_date(start_date)
    if end_date is not None and not isinstance(end_date, date):
        end_date = parse_date(end_date)

    graph_paths = sorted(Path("16TB/graphs_cleaned").glob("*.pickle"))

    # Filter files based on date range
    if start_date is not None or end_date is not None:
        filtered_paths = []
        for graph_path in graph_paths:
            _, graph_date = parse_filename(graph_path)
            if (start_date is None or graph_date >= start_date) and (end_date is None or graph_date <= end_date):
                filtered_paths.append(graph_path)
        graph_paths = filtered_paths

    return graph_paths


class GraphEdgeIterator:
    def __init__(
            self, start_date: date | str | None = None, end_date: date | str | None = None):
        # Iterator over file paths
        self.graph_path_iterator = iter(get_graph_paths(start_date=start_date, end_date=end_date))
        self.buffer = [None, None]  # Two graph buffers
        self.current_edges: Iterator[tuple[int, int, int]] = iter([])  # Empty iterator initially
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)  # Async executor

        # Load the first two graphs
        self._trigger_buffer(0)
        self._trigger_buffer(1)
        self._resolve_buffer(0)

    def _trigger_buffer(self, index):
        """Load the next graph into the buffer asynchronously."""
        try:
            next_graph_path = next(self.graph_path_iterator)  # Get the next file path
            future = self.executor.submit(read_graph, next_graph_path)
            self.buffer[index] = future
        except StopIteration:
            self.buffer[index] = None  # No more graphs to load

    def _resolve_buffer(self, index):
        # Wait for the graph to load
        self.current_edges = iter(self.buffer[index].result().edges)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            # Try to get the next edge from the current graph's edges
            return next(self.current_edges)
        except StopIteration:
            # Current graph is exhausted
            if self.buffer[0] is None:
                raise StopIteration  # No more graphs to process

            # Move buffer[1] to buffer[0] and load the next graph
            self.buffer[0] = self.buffer[1]
            print("swapped buffer")
            if self.buffer[0] is not None:
                self._resolve_buffer(0)
            else:
                raise StopIteration

            # Start loading the next graph in buffer[1]
            self._trigger_buffer(1)

            # Retry to get the next edge
            return next(self.current_edges)


def iteration_logging(
        generator: Iterable[Any],
        log_path: Path,
        log_interval: int = 1,
        max_time: int = 60 * 60
) -> Generator[Any, None, None]:
    """
    Tracks iterations over a generator and logs progress every log_interval seconds,
    along with memory usage of the program. Stops the iteration if the maximum time
    is reached.

    Args:
        generator (Iterable): The generator that yields the items to iterate over.
        log_path (Path): The path to save the log file.
        log_interval (int): The time interval in seconds to log progress.
        max_time (int): The maximum time in seconds to allow iteration.
    """
    # Initialize timing details
    start_time = time.time()  # Record the starting time
    last_log_time = start_time  # Keep track of the last log time
    total_iterations = 0  # Total iterations done so far
    process = psutil.Process()  # To track memory usage of the current process

    with log_path.open("w") as log_file:
        # Header for the log file
        log_file.write("time_seconds,iterations_total,iterations_rate,memory_usage_bytes\n")

        for item in generator:
            current_time = time.time()
            total_iterations += 1

            # Check if maximum time has been reached
            if current_time - start_time >= max_time:
                break  # Stop iteration if the maximum time is exceeded

            # Check if the log interval has passed
            if current_time - last_log_time >= log_interval:
                elapsed_time = current_time - start_time
                rate = total_iterations / elapsed_time  # Iterations per second

                # Get the memory usage in bytes
                memory_usage_bytes = process.memory_info().rss

                # Log progress: time, iterations, rate, memory usage
                line = f"{elapsed_time},{total_iterations},{rate},{memory_usage_bytes}\n"
                log_file.write(line)

                last_log_time = current_time  # Reset the last log time

            yield item  # Yield the item as usual for normal iteration flow


def parse_arguments(**kwargs) -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Cycle Detection in Temporal Graphs (v0.1)")

    # Convert kwargs into a list of command-line arguments
    simulated_args = []
    for key, value in kwargs.items():
        if isinstance(value, bool) and value:  # Handle flags (true means the flag is present)
            simulated_args.append(f"--{key}")
        elif not isinstance(value, bool):  # Handle arguments with values
            simulated_args.extend([f"--{key}", str(value)])

    # Optional arguments with defaults
    parser.add_argument(
        "-f", "--root_file_arg", type=str, default="NA",
        help="Path of the root file (default: NA)."
    )
    parser.add_argument(
        "-e", "--projected_element_count", type=int, default=1000,
        help="Projected element count (default: 1000)."
    )
    parser.add_argument(
        "-w", "--window", type=int, default=1,
        help="Time window in hours (default: 1)."
    )
    parser.add_argument(
        "-p", "--cleanUpLimit", type=int, default=10000,
        help="Clean up size (default: 10000)."
    )
    parser.add_argument(
        "-a", "--rootAlgo", type=int, default=1,
        help="Algorithm to find root (0 for old, 1 for new, default: 1)."
    )
    parser.add_argument(
        "-r", "--reverseDirection", action="store_true",
        help="Reverse direction of edge (default: False)."
    )
    parser.add_argument(
        "-z", "--isCompressed", action="store_true",
        help="Indicate if the root node is compressed (default: False)."
    )
    parser.add_argument(
        "-c", "--is_candidates_provided", action="store_true",
        help="Indicate if a candidate list is provided (default: True)."
    )
    parser.add_argument(
        "-l", "--cycleLength", type=int, default=80,
        help="Cycle length (default: 80)."
    )
    parser.add_argument(
        "-b", "--use_bundle", action="store_true",
        help="Use bundle (default: False)."
    )

    # New arguments: start_date and end_date
    parser.add_argument(
        "--start_date", type=parse_date, default=None,
        help="Start date in format YYYY-MM-DD (default: None)."
    )
    parser.add_argument(
        "--end_date", type=parse_date, default=None,
        help="End date in format YYYY-MM-DD (default: None)."
    )

    # Use simulated_args instead of args
    return parser.parse_args(simulated_args)


def to_cli_arguments(args: argparse.Namespace) -> list[str]:
    """
    Convert argparse.Namespace to a list of CLI arguments.
    Handles flags (boolean arguments) correctly.
    """
    cli_arguments = []
    for key, value in vars(args).items():
        if isinstance(value, bool):  # Handle flags
            if value:  # Include flag only if True
                cli_arguments.append(f"--{key}")
        elif value is not None:  # Handle arguments with values
            cli_arguments.extend([f"--{key}", str(value)])
    return cli_arguments
