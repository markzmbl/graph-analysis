from __future__ import annotations
import argparse
import io
import pickle
import sys
import time
from bisect import bisect_right
from collections import defaultdict, deque
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
from sortedcontainers import SortedList, SortedDict
from sympy import Interval, FiniteSet, EmptySet
from tqdm.auto import tqdm
import plotly.graph_objects as go



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
