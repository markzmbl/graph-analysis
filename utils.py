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
from typing import Generator, Iterable, Any, Hashable

from IPython.core.display_functions import display
from pydantic.alias_generators import to_camel
from setuptools.config.pyprojecttoml import load_file
from sortedcontainers import SortedList
from sympy import Interval
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
            u, v, current_time = next(self.current_edges)
            return u, v, current_time
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
            return next(self)


class GraphCycleIterator:
    def __init__(
            self,
            edge_iterator: Iterator[tuple[Hashable, Hashable, int]],
            omega: int = 10,
            prune_interval: int = 1_000
    ):
        self.edge_iterator = edge_iterator
        self.iteration_count = 0
        self.prune_interval = prune_interval

        # Graph Reverse Reachability
        self.omega = omega
        self.reverse_reachability = {}

        # Candidate Seeds

        def seed_sort_key(edge):
            v, start, end, _ = edge
            return v, start, -end

        self.seeds = SortedList(key=seed_sort_key)

    def _prune_reverse_reachability_set(self, vertex, current_time):
        self.reverse_reachability[vertex] = {
            (x, time_x) for x, time_x in self.reverse_reachability[vertex]
            if time_x > current_time - self.omega
        }

    def _update_reverse_reachability(self, u, v, current_time_u):
        # Ignore trivial self edges
        if u == v:
            return

        # Initialization of empty set if necessary
        self.reverse_reachability.setdefault(v, set())

        self.reverse_reachability[v].add((u, current_time_u))
        if u in self.reverse_reachability:
            # Prune old edges of u's reverse reachability
            self._prune_reverse_reachability_set(u, current_time_u)

            # Update: because (u, v) exists, all reachable to u are also reachable to v
            self.reverse_reachability[v].update(self.reverse_reachability[u])
            to_delete = set()
            for w, time_w in self.reverse_reachability[u]:
                if w == v:
                    candidates = {c for c, time_c in self.reverse_reachability[u] if time_c > time_w}
                    if len(candidates) > 0:
                        # TODO: Algorithm 3 Combining Root Node Candidate
                        self.seeds.add((w, time_w, current_time_u, candidates))
                        # interval_w = Interval(time_w, current_time_u)
                        # self.seeds.setdefault(w, {})
                        # for interval in self.seeds[w]:
                        #     if interval_w.is_subset(interval):
                        #         self.seeds[w][interval].update(candidates)
                        #         break
                        # else:
                        #     self.seeds[w][interval_w] = candidates
                    to_delete.add((w, time_w))
            self.reverse_reachability[u].difference_update(to_delete)

    def __iter__(self):
        return self

    def run(self):
        for u, v, current_time in self.edge_iterator:
            self.iteration_count += 1
            self._update_reverse_reachability(u, v, current_time)

            # Cleanup of old edges
            if self.iteration_count % self.prune_interval == 0:
                for w in self.reverse_reachability:
                    self._prune_reverse_reachability_set(w, current_time)


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
        def create_figure_widget(name: str, yaxis_title: str) -> go.FigureWidget:
            figure = go.FigureWidget()
            figure.add_trace(go.Scatter(name=name, mode="lines"))
            figure.update_layout(
                xaxis_title="seconds",
                yaxis_title=yaxis_title,
            )
            figure.data[0].x = []
            figure.data[0].y = []
            return figure

        figures = {
            "iterations": create_figure_widget("Iterations", "iterations"),
            "rate": create_figure_widget("Iteration rate", "iteration rate"),
            "memory": create_figure_widget("Memory usage", "memory usage (GB)"),
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


def generate_seeds(edges, omega, prune_interval=1_000):
    reachability = {}  # Summaries for each vertex

    for i, (u, v, time_u) in enumerate(edges):
        # Trivial self edges are ignored
        if u == v:
            continue

        if v not in reachability:
            # Initialization of empty set if necessary
            reachability[v] = set()

        # Update: v is reachable by u at current_time t
        reachability[v].add((u, time_u))

        # Check the reachability of u
        if u in reachability:

            reachability[u] = {(w, time_w) for w, time_w in reachability[u] if time_w > time_u - omega}
            # Update: reachability(v) with entries from reachability(u)
            reachability[v].update(reachability[u])

            # Iterate over copies to avoid modification issues
            for vertex_b, tb in list(reachability[v]):
                if vertex_b == v:
                    if tb < time_u:
                        # Construct candidates C
                        candidates = {c for (c, tc) in reachability[u] if tc > tb}
                        candidates.add(v)
                        # Output the seed
                        yield v, (tb, time_u), candidates
                    # Remove (v, tb) from reachability(v)
                    reachability[v].remove((v, tb))

        # Time to prune all summaries
        if i % prune_interval == 0:
            for vertex in reachability:
                reachability[vertex] = {(w, time_w) for w, time_w in reachability[vertex] if time_w > time_u - omega}
