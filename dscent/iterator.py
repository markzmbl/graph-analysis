from __future__ import annotations

import csv
import io
import time
from collections.abc import Iterator, Generator
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore
from time import monotonic
from typing import TextIO

import humanfriendly
import psutil
from humanfriendly import parse_size
from tqdm import tqdm

from dscent.graph import BundledCycle
from dscent.seed import Seed, SeedGenerator, SeedExplorer
from dscent.types_ import (
    TransactionBlock,
    TargetInteraction,
    EdgeInteraction,
    Timedelta, Timestamp
)


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers: int, queue_size: int):
        super().__init__(max_workers=max_workers)
        self._semaphore = Semaphore(max_workers + queue_size)

    def submit(self, fn, *args, **kwargs):
        self._semaphore.acquire()
        future = super().submit(fn, *args, **kwargs)
        future.add_done_callback(lambda _: self._semaphore.release())
        return future


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            interactions: Iterator[EdgeInteraction],
            omega: Timedelta = 10,
            garbage_collection_max: int | str = "32G",
            log_stream: io.StringIO | TextIO | None = None,
            logging_interval: int = 60,
            yield_seeds: bool = False,
            progress_bar: bool = False,
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum timestamp
        window, and a prune data_interval.
        """
        # General
        self._use_tqdm = progress_bar
        interactions = iter(interactions)
        if self._use_tqdm:
            interactions = tqdm(interactions, unit_scale=True, smoothing=1, position=0)
        self._interactions = interactions
        self._iteration_count: int = 0  # Track number of processed edges

        # Seed Generation
        self._seed_generator = SeedGenerator(omega=omega)
        # Seed Exploration
        self._seed_explorer = SeedExplorer(omega=omega)

        # Time Threshold
        self._omega = omega

        # Memory Monitoring
        self._process = psutil.Process()
        self._max_bytes = parse_size(garbage_collection_max)  # max bytes for pruning old data

        # Logging
        # Seeds
        self.yield_seeds = yield_seeds
        # Stream
        if log_stream is None:
            self._logging = False
            self._start_time = None
        else:
            self._start_time = monotonic()
            self._logging = True
            self._logging_interval = logging_interval
            self._csv_writer = csv.writer(log_stream)
            self._write_log_header()
            self._last_log_time = self._start_time

    def _get_memory_usage(self) -> int:
        """
        Get the current memory usage of the process.
        :return: Memory usage in bytes.
        """
        return self._process.memory_info().rss

    def _memory_limit_exceeded(self) -> bool:
        """
        Check if the memory limit is exceeded.
        :return: True if memory limit is exceeded, False otherwise.
        """
        return self._get_memory_usage() > self._max_bytes

    def cleanup(self, current_time: Timestamp) -> None:
        before = self._get_memory_usage()
        self._seed_generator.cleanup(current_time=current_time)
        self._seed_explorer.cleanup()
        print(
            f"Memory cleanup: {humanfriendly.format_size(before)} -> "
            f"{humanfriendly.format_size(self._get_memory_usage())}"
        )

    def _get_log_line(
            self,
            header=False
    ) -> list[str | float]:
        current_time = time.monotonic()
        exploration_tasks = self._seed_explorer.get_running_tasks()
        running = sum(task.done() for task in exploration_tasks.values())
        fields = {
            "time_seconds": current_time,
            "iterations_total": self._iteration_count,
            "iterations_rate": self._iteration_count / (current_time - self._start_time),
            "memory_usage_bytes": self._get_memory_usage(),
            "task_queue_size": len(exploration_tasks),
            "running_tasks": running,
        }
        if header:
            return list(fields.keys())
        return list(fields.values())

    @staticmethod
    def _format_log_line(values: list[str]) -> str:
        return ",".join(str(value) for value in values) + "\n"

    def _write_log_header(self):
        header = self._get_log_line(header=True)
        self._csv_writer.writerow(header)

    def _log(self, log_time):
        log_line = self._get_log_line()
        self._csv_writer.writerow(log_line)
        self._last_log_time = log_time

    def _process_batch(self, batch: TransactionBlock):
        block_timestamp = batch.timestamp
        for target, sources in batch.items():
            # Construct interaction to single target from multiple sources
            target_interaction = TargetInteraction(
                target=target,
                sources=sources,
                timestamp=block_timestamp,
            )
            # Submit Seed Generation Task
            self._seed_generator.submit(interaction=target_interaction)

    def _start_exploration_tasks(self, upper_limit: Timestamp | None = None):
        # Try to gather new exploration tasks
        for primed_seed in self._seed_generator.pop_primed_seeds(upper_limit=upper_limit):
            # Submit exploration task of primed seeds
            self._seed_explorer.submit(primed_seed=primed_seed)

    def _explored_cycles(self) -> Generator[BundledCycle | tuple[Seed, BundledCycle], None, None]:
        detected_cycles = self._seed_explorer.pop_detected_cycle_graphs(include_seeds=self.yield_seeds)
        if self.yield_seeds:
            for seed, cycle_graphs in detected_cycles.items():
                for graph in cycle_graphs:
                    yield seed, graph  # Yield seed and cycle graph
        else:
            for cycle in detected_cycles:
                yield cycle  # Yield cycle graph

    def __iter__(self) -> Generator[BundledCycle | tuple[Seed, BundledCycle], None, None]:
        transaction_block: TransactionBlock | None = None  # Placeholder for the first transaction block
        for edge in self._interactions:

            # --- Seed Generation ---

            # Track iteration count
            self._iteration_count += 1
            # Extract Edge information
            source, target, current_time, edge_data = edge
            # Skip trivial self loops
            if source == target:
                continue
            # Add edge to the transaction graph
            self._seed_explorer.add_edge(source, target, timestamp=current_time, **edge_data)
            # First iteration: Initialize transaction block
            if transaction_block is None:
                transaction_block = TransactionBlock(timestamp=current_time)
            # Check if the current time is different from the last processed time
            if current_time != transaction_block.timestamp:
                # Process the current batch before moving on
                self._seed_generator.process_batch(batch=transaction_block)
                # Reset batch and update batch_id
                transaction_block = TransactionBlock(timestamp=current_time)

                # --- Seed Exploration ---

                # Start exploration tasks for primed seeds
                self._start_exploration_tasks(upper_limit=current_time - self._omega)
                # Yield results from finished explorations
                yield from self._explored_cycles()

                # --- Memory Management ---

                # Check if memory is exceeded
                if self._memory_limit_exceeded():
                    # Cleanup Memory
                    self.cleanup(current_time=current_time)
                    if self._memory_limit_exceeded():  # Check again after cleanup
                        raise MemoryError("Out of memory.")

            # This always runs (either for old or new batch)
            transaction_block[target].append(source)

            # --- Logging ---

            if self._logging:
                now = monotonic()
                # Check if Logging interval is exceeded
                if now - self._last_log_time > self._logging_interval:
                    self._log(log_time=now)

        # Start final exploration tasks
        self._start_exploration_tasks(upper_limit=None)
        # Wait for final exploration tasks to finish
        self._seed_explorer.wait()
        # Yield final found cycles
        yield from self._explored_cycles()
