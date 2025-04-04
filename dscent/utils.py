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
) -> Generator[Any, None, None]:
    # Initialize _logging variables
    start_time = time.time()
    last_log_time = start_time
    total_iterations = 0
    process = psutil.Process()

    # Write header to log stream if provided
    if log_stream:
        log_stream.write("time_seconds,iterations_total,iterations_rate,memory_usage_bytes\n")

    # Main iteration loop
    for item in generator:
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

            last_log_time = current_time

        yield item  # Yield the current item for iteration flow
