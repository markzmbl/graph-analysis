import argparse
import io
import pickle
import sys
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, Future
from contextlib import nullcontext
from datetime import datetime, date

import networkx as nx
import numpy as np
import psutil
from pathlib import Path
from typing import Generator, Iterable, Any

from IPython.core.display_functions import display
from pydantic.alias_generators import to_camel
from setuptools.config.pyprojecttoml import load_file
from tqdm.auto import tqdm
import plotly.graph_objects as go

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
        self,
        start_date: date | str | None = None,
        end_date: date | str | None = None,
        buffer_count: int = 2,  # Number of graph buffers
    ):
        # Iterator over file paths
        self.graph_path_iterator = iter(get_graph_paths(start_date=start_date, end_date=end_date))
        self.buffer_count = buffer_count
        self.buffer = [None] * self.buffer_count  # Graph buffers
        self.current_edges: Iterator[tuple[int, int, int]] = iter([])  # Empty iterator initially
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)  # Async executor

        # Preload buffers
        for i in range(min(self.buffer_count, len(self.buffer))):
            self._trigger_buffer(i)
        self._resolve_buffer(0)  # Load the first graph into the edges iterator

    def _trigger_buffer(self, index):
        """Load the next graph into the buffer asynchronously."""
        try:
            next_graph_path = next(self.graph_path_iterator)  # Get the next file path
            future = self.executor.submit(read_graph, next_graph_path)
            self.buffer[index] = future
        except StopIteration:
            self.buffer[index] = None  # No more graphs to load

    def _resolve_buffer(self, index):
        """Wait for the graph to load and set the current edges iterator."""
        self.current_edges = iter(self.buffer[index].result().edges)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            # Try to get the next edge from the current graph's edges
            return next(self.current_edges)
        except StopIteration:
            # Current graph is exhausted
            if all(buf is None for buf in self.buffer):
                raise StopIteration  # No more graphs to process

            # Shift buffers
            for i in range(self.buffer_count - 1):
                self.buffer[i] = self.buffer[i + 1]
            self.buffer[-1] = None

            # Load the next graph into the last buffer
            if self.buffer[0] is not None:
                self._resolve_buffer(0)
            else:
                raise StopIteration

            self._trigger_buffer(self.buffer_count - 1)

            # Retry to get the next edge
            return next(self.current_edges)


def iteration_logging(
    generator: Iterable[Any],
    log_interval: int = 1,
    max_time: int = 60 * 60,
    log_stream: io.IOBase | None = None,
    progress_bar: bool = False,
    plot: bool = False,
) -> Generator[Any, None, None]:
    # Wrap generator with progress bar if needed
    wrapped_generator = tqdm(generator) if progress_bar else generator

    # Initialize figures
    figures = {}
    if plot:
        def create_figure_widget(name: str, xaxis_title: str, yaxis_title: str) -> go.FigureWidget:
            figure = go.FigureWidget()
            figure.add_trace(go.Scatter(name=name, mode="lines"))
            figure.update_layout(
                xaxis_title=xaxis_title,
                # xaxis=dict(tickmode='linear', dtick=1),
                yaxis_title=yaxis_title,
            )
            figure.data[0].x = []
            figure.data[0].y = []
            return figure

        figures = {
            "iterations": create_figure_widget("Iterations", "seconds", "iterations"),
            "rate": create_figure_widget("Iteration rate", "seconds", "iteration rate"),
            "memory": create_figure_widget("Memory usage", "seconds", "memory usage (GB)"),
        }

        # Display figures initially
        for fig in figures.values():
            display(fig)

    # Initialize logging variables
    start_time = time.time()
    last_log_time = start_time
    total_iterations = 0
    process = psutil.Process()

    # Write header to log stream if provided
    if log_stream:
        log_stream.write("time_seconds,iterations_total,iterations_rate,memory_usage_bytes\n")

    # Main iteration loop
    for item in wrapped_generator:
        current_time = time.time()
        total_iterations += 1

        # Stop if max_time is exceeded
        if current_time - start_time >= max_time:
            break

        # Log at specified intervals
        if current_time - last_log_time >= log_interval:
            elapsed_time = current_time - start_time
            rate = total_iterations / elapsed_time
            memory_usage_bytes = process.memory_info().rss

            # Log data to stream
            if log_stream:
                log_stream.write(f"{elapsed_time},{total_iterations},{rate},{memory_usage_bytes}\n")

            # Update plots if enabled
            if plot:
                elapsed_seconds = round(elapsed_time)
                updates = {
                    "iterations": total_iterations,
                    "rate": rate,
                    "memory": memory_usage_bytes / 1e9,  # Convert bytes to GB
                }

                for key, value in updates.items():
                    figure = figures[key]
                    # Append new data to the plot
                    with figure.batch_update():
                        figure.data[0].x += (elapsed_seconds,)
                        figure.data[0].y += (value,)

            last_log_time = current_time

        yield item  # Yield the current item for iteration flow


def parse_arguments(**kwargs) -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description="Cycle Detection in Temporal Graphs (v0.1)")

    # Convert kwargs into a list of command-line arguments
    simulated_args = []
    for key, value in kwargs.items():
        key = key.replace("_", "-")
        if isinstance(value, bool) and value:  # Handle flags (true means the flag is present)
            simulated_args.append(f"--{key}")
        elif not isinstance(value, bool):  # Handle arguments with values
            simulated_args.extend([f"--{key}", str(value)])

    # Optional arguments with defaults
    parser.add_argument(
        "-f", "--root-file-arg", type=str, default="NA",
        help="Path of the root file (default: NA)."
    )
    parser.add_argument(
        "-e", "--projected-element-count", type=int, default=1000,
        help="Projected element count (default: 1000)."
    )
    parser.add_argument(
        "-w", "--window", type=int, default=1,
        help="Time window in hours (default: 1)."
    )
    parser.add_argument(
        "-p", "--clean-up-limit", type=int, default=10000,
        help="Clean up size (default: 10000)."
    )
    parser.add_argument(
        "-a", "--root-algo", type=int, default=1,
        help="Algorithm to find root (0 for old, 1 for new, default: 1)."
    )
    parser.add_argument(
        "-r", "--reverse-direction", action="store_true",
        help="Reverse direction of edge (default: False)."
    )
    parser.add_argument(
        "-z", "--is-compressed", action="store_true",
        help="Indicate if the root node is compressed (default: False)."
    )
    parser.add_argument(
        "-c", "--is-candidates-provided", action="store_true",
        help="Indicate if a candidate list is provided (default: True)."
    )
    parser.add_argument(
        "-l", "--cycle-length", type=int, default=80,
        help="Cycle length (default: 80)."
    )
    parser.add_argument(
        "-b", "--use-bundle", action="store_true",
        help="Use bundle (default: False)."
    )

    # New arguments: start_date and end_date
    parser.add_argument(
        "--start-date", type=parse_date, default=None,
        help="Start date in format YYYY-MM-DD (default: None)."
    )
    parser.add_argument(
        "--end-date", type=parse_date, default=None,
        help="End date in format YYYY-MM-DD (default: None)."
    )

    # Use simulated_args instead of args
    return parser.parse_args(simulated_args)


def to_cli_arguments(args: argparse.Namespace) -> list[str]:
    """
    Convert argparse.Namespace to a list of CLI arguments.
    Handles flags (boolean arguments) correctly.
    """
    del args.start_date, args.end_date,
    cli_arguments = []
    for key, value in vars(args).items():
        key = key.replace("_", "-")
        if isinstance(value, bool):  # Handle flags
            if value:  # Include flag only if True
                cli_arguments.append(f"--{key}")
        elif value is not None:  # Handle arguments with values
            cli_arguments.extend([f"--{key}", str(value)])
    return cli_arguments


def generate_seeds(edges, omega, prune_interval=1_000):
    """

    :param edges:
    :param omega:
    :param prune_interval:
    :return: All vertexs s, time stamps t_start and t_end, and a candidates set such that there exists a loop from s to s
        using only vertexs in C starting at t_start and ending at t_end
    """
    S = {}  # Summaries for each vertex

    for i, (a, b, t) in enumerate(edges):
        if a == b:
            continue

        # Ensure S(b) exists
        if b not in S:
            S[b] = set()
        # Add (a, t) to S(b)
        S[b].add((a, t))

        if a in S:
            # Prune outdated entries from S(a)
            S[a] = {(x, tx) for (x, tx) in S[a] if tx > t - omega}
            # Update S(b) with entries from S(a)
            S[b].update(S[a])

            # Iterate over copies to avoid modification issues
            for vertex_b, tb in list(S[b]):
                if vertex_b == b:
                    # Construct candidates C
                    candidates = {c for (c, tc) in S[a] if tc > tb}
                    candidates.add(b)
                    # Output the seed
                    yield b, (tb, t), candidates
                    # Remove (b, tb) from S(b)
                    S[b].remove((b, tb))

        # Time to prune all summaries
        if i % prune_interval == 0:
            for vertex in S:
                S[vertex] = {(y, ty) for (y, ty) in S[vertex] if ty > t - omega}
