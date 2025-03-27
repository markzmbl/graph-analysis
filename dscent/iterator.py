from __future__ import annotations

import csv
import io
import time
from collections.abc import Iterator, Generator
from concurrent.futures import Future, wait, FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat
from time import monotonic
from typing import TextIO

import psutil
from humanfriendly import parse_size

from dscent.graph import TransactionGraph, ExplorationGraph, BundledCycle
from dscent.seed import Seed, SeedGenerator
from dscent.types_ import TimeDelta, Interaction


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            interactions: Iterator[Interaction],
            omega: TimeDelta = 10,
            max_workers: int = 4,
            threaded: bool = False,
            garbage_collection_max: int | str = "16G",
            log_stream: io.StringIO | TextIO | None = None,
            logging_interval: int = 60,
            yield_seeds: bool = False,
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum timestamp
        window, and a prune data_interval.
        """
        self._interactions = iter(interactions)  # Stream of edges
        self._iteration_count: int = 0  # Track number of processed edges

        self._process = psutil.Process()
        self._max_bytes: int = parse_size(garbage_collection_max)  # max bytes for pruning old data

        self._max_workers = max_workers  # Maximum number of active workers
        self._parallel = self._max_workers > 0
        self._threaded = threaded

        self._task_pool: ProcessPoolExecutor | None = None
        self._running_tasks: dict[Seed, Future] = {}
        self._completed_tasks: dict[Seed, Future] = {}
        self._explored_seeds: set[Seed] = set()

        self._seed_generator = SeedGenerator(omega=omega)
        self._yield_seeds = yield_seeds

        # Dynamic directed sub_graph with edge removal enabled
        self._transaction_graph = TransactionGraph()

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

    def _initialize_task_pool(self):
        if self._threaded:
            self._task_pool = ThreadPoolExecutor(max_workers=self._max_workers)
        else:
            self._task_pool = ProcessPoolExecutor(max_workers=self._max_workers)

    def _update_completed_tasks(self):
        for seed, running_task in list(self._running_tasks.items()):
            if running_task.done():
                # Pop from _running_tasks, put into _completed_tasks
                done_task = self._running_tasks.pop(seed)
                self._completed_tasks[seed] = done_task

    def _await_task(self, all_tasks: bool = False) -> None:
        if not all_tasks:
            wait(self._running_tasks.values(), return_when=FIRST_COMPLETED)
        else:
            wait(self._running_tasks.values())

    def _submit_exploration_task(self, sub_graph: ExplorationGraph, seed: Seed):
        if self._parallel:
            # Wait if necessary
            while len(self._running_tasks) >= self._max_workers:
                self._await_task()
            task = self._task_pool.submit(sub_graph.simple_cycles, seed.next_begin)
        else:
            # Mock Future
            task = Future()
            task.set_result(sub_graph.simple_cycles(seed.next_begin))
        self._running_tasks[seed] = task

    def _run_new_exploration_tasks(self, complete: bool = False):
        while self._seed_generator.has_seeds():
            seed = self._seed_generator.pop_seed()
            root_vertex = seed.root
            # Subgraph View
            graph_view = self._transaction_graph.time_slice(
                begin=seed.interval.begin,
                end=seed.interval.end,
                closed=True,
                nodes=seed.candidates
            )
            # Make Copy
            sub_graph = ExplorationGraph(graph_view, root_vertex=root_vertex)
            self._submit_exploration_task(sub_graph=sub_graph, seed=seed)
        if complete:
            self._await_task(all_tasks=True)

    def _found_cycles(self) -> Iterator[BundledCycle | tuple[Seed, BundledCycle]]:
        # Get currently done tasks
        self._update_completed_tasks()
        for seed, completed_task in self._completed_tasks.items():
            found_cycles = list(completed_task.result())
            if not self._yield_seeds:
                yield from found_cycles
            else:
                yield from zip(repeat(seed), found_cycles)
        self._explored_seeds.update(self._completed_tasks)
        self._completed_tasks.clear()

    def _get_memory_usage(self) -> int:
        return self._process.memory_info().rss

    def _prune_transaction_graph(self):
        # If there are any primed seeds or running tasks, determine the minimum data_interval begin
        # Keep Track of minimum needed Graph interactions
        thresholds = []
        # From finished tasks take the latest begin
        if len(self._explored_seeds) > 0:
            thresholds.append(max(seed.interval.begin for seed in self._explored_seeds))
            self._explored_seeds.clear()

        # From running and future tasks track the earliest start
        thresholds += [
            seed.interval.begin
            for seed in
            list(self._running_tasks) + self._seed_generator.seeds
        ]
        if len(thresholds) == 0:
            return

        minimum_graph_begin = min(thresholds)
        # If the updated minimum sub_graph begin exceeds the transaction sub_graph's begin timestamp,
        if self._transaction_graph.begin() < minimum_graph_begin:
            # prune the sub_graph to remove data older than the new minimum
            self._transaction_graph.prune(lower_time_limit=minimum_graph_begin)

    def cleanup(self):
        self._seed_generator.cleanup()
        self._prune_transaction_graph()

    def _get_log_line(
            self,
            header=False
    ) -> list[str | float]:
        current_time = time.monotonic()
        fields = {
            "time_seconds": current_time,
            "iterations_total": self._iteration_count,
            "iterations_rate": self._iteration_count / (current_time - self._start_time),
            "memory_usage_bytes": self._get_memory_usage(),
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

    def __iter__(self) -> Generator[BundledCycle | tuple[Seed, BundledCycle], None, None]:
        if self._parallel and self._task_pool is None:
            self._initialize_task_pool()

        for edge in self._interactions:
            source, target, timestamp, edge_data = edge
            interaction = Interaction(source, target, timestamp)

            # Skip trivial self loops
            if source == target:
                continue

            self._iteration_count += 1  # Track iteration count

            self._seed_generator.update(interaction)
            self._transaction_graph.add_edge(source, target, key=timestamp, **edge_data)

            self._run_new_exploration_tasks()
            yield from self._found_cycles()

            if self._get_memory_usage() > self._max_bytes == 0:
                self.cleanup()

            if self._logging:
                now = monotonic()
                if now - self._last_log_time > self._logging_interval:
                    self._log(log_time=now)

        self._run_new_exploration_tasks(complete=True)
        yield from self._found_cycles()
        if self._parallel:
            self._task_pool.shutdown()
