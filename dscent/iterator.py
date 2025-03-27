from __future__ import annotations

import csv
import io
import time
from collections import defaultdict
from collections.abc import Iterator, Hashable, Generator
from concurrent.futures import Future, wait, FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass
from itertools import repeat
from time import monotonic
from typing import TextIO

import psutil
from humanfriendly import parse_size
from intervaltree import IntervalTree
from intervaltree import Interval as DataInterval

from dscent.graph import TransactionGraph, ExplorationGraph, BundledCycle
from dscent.types_ import (
    Vertex, TimeDelta, Interaction, SingleTimedVertex, Timestamp, Interval
)
from dscent.reachability import DirectReachability


class _Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next data_interval, if available


@dataclass(frozen=True)
class _Seed:
    root: Vertex
    interval: Interval
    candidates: frozenset[Vertex]
    next_begin: Timestamp

    @staticmethod
    def construct(root: Vertex, data_interval: DataInterval[Timestamp, Timestamp, _Candidates]):
        begin, end, candidates = data_interval
        candidates.add(root)
        next_begin = candidates.next_begin

        return _Seed(
            root=root,
            interval=Interval(begin, end),
            candidates=frozenset(candidates),
            next_begin=next_begin,
        )


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
        self.omega: TimeDelta = omega  # Maximum timestamp window for relevant edges

        self.max_workers = max_workers  # Maximum number of active workers
        self.parallel = self.max_workers > 0
        self.threaded = threaded

        self._task_pool: ProcessPoolExecutor | None = None
        self._running_tasks: dict[_Seed, Future] = {}
        self._completed_tasks: dict[_Seed, Future] = {}

        # Reverse reachability mapping: root -> sorted set of (_lower_time_limit, predecessor) pairs
        self._reverse_reachability: dict[Vertex, DirectReachability] = defaultdict(DirectReachability)

        # Interval tracking for candidate cycles: root -> IntervalTree
        self._seed_intervals: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)

        # Seeds which can not grow any further and are primed for exploration
        self._primed_seeds: list[_Seed] = []
        self.yield_seeds = yield_seeds

        # Dynamic directed sub_graph with edge removal enabled
        self._transaction_graph = TransactionGraph()

        if log_stream is None:
            self.logging = False
            self.start_time = None
        else:
            self.start_time = monotonic()
            self.logging = True
            self.logging_interval = logging_interval
            self._csv_writer = csv.writer(log_stream)
            self._write_log_header()
            self._last_log_time = self.start_time

    def _initialize_task_pool(self):
        if self.threaded:
            self._task_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        else:
            self._task_pool = ProcessPoolExecutor(max_workers=self.max_workers)

    def _update_reverse_reachability(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        # (a, b, t) ∈ E
        source, target, current_timestamp, _ = interaction
        target_reverse_reachability = self._reverse_reachability[target]

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(SingleTimedVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.trim_before(self._lower_time_limit, strict=False)

        if source not in self._reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self._reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.trim_before(self._lower_time_limit, strict=False)
        if len(source_reverse_reachability) == 0:
            del self._reverse_reachability[source]
            return

        # Propagate reachability
        # S(b) ← S(b) ∪ S(a)
        target_reverse_reachability |= source_reverse_reachability

        # for (b, tb) ∈ S(b) do
        cyclic_reachability = DirectReachability(
            [v for v in target_reverse_reachability if v.vertex == target]
        )
        if len(cyclic_reachability) == 0:
            return

        # {c ∈ S(a), tc > tb}
        for cyclic_reachable in cyclic_reachability:
            candidate_reachability = DirectReachability(source_reverse_reachability)
            candidate_reachability.trim_before(lower_limit=cyclic_reachable.timestamp)

            if len(candidate_reachability) == 0:
                continue

            # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
            candidates = _Candidates([source])
            candidates.update(c.vertex for c in candidate_reachability)

            if len(candidates) > 1:
                candidates.next_begin = cyclic_reachable.timestamp + self.omega
                # Output (b, [tb, t], C)
                self._seed_intervals[target][cyclic_reachable.timestamp: current_timestamp] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = DirectReachability(
            v for v in target_reverse_reachability if v not in cyclic_reachability
        )

    def _combine_seeds(self, v: Hashable) -> None:
        """
        Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
        """
        if v not in self._seed_intervals:
            return

        interval_tree = self._seed_intervals[v]
        combined_interval_tree = IntervalTree()

        while len(interval_tree) > 1:
            first_interval_begin = interval_tree.begin()
            upper_interval_limit = first_interval_begin + self.omega  # Define merge range

            compatible_intervals = interval_tree.envelop(first_interval_begin, upper_interval_limit)

            if len(compatible_intervals) == 1:
                compatible_interval = next(iter(compatible_intervals))
                combined_candidates = _Candidates(compatible_interval.data)
                combined_interval_end = compatible_interval.end
            else:
                combined_candidates = _Candidates()
                combined_interval_end = float("-inf")

                for compatible_interval in compatible_intervals:
                    combined_candidates.update(compatible_interval.data)
                    combined_interval_end = max(combined_interval_end, compatible_interval.end)  # Extend data_interval

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)  # Remove merged intervals
            if interval_tree:
                combined_candidates.next_begin = interval_tree.begin()  # set next data_interval start
            else:
                combined_candidates.next_begin = upper_interval_limit  # No more intervals

            # Store merged data_interval
            combined_interval_tree[first_interval_begin: combined_interval_end] = combined_candidates

        if combined_interval_tree:
            self._seed_intervals[v] = combined_interval_tree  # Update _seed_intervals

    def _update_primed_seeds(self, complete: bool = False):
        end = float("inf") if complete else self._lower_time_limit
        lower_time_range = (0, end)
        for vertex, interval_tree in self._seed_intervals.items():
            for data_interval in interval_tree.envelop(*lower_time_range):
                seed = _Seed.construct(root=vertex, data_interval=data_interval)
                self._primed_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)

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

    def _submit_exploration_task(self, sub_graph: ExplorationGraph, seed: _Seed):
        if self.parallel:
            # Wait if necessary
            while len(self._running_tasks) >= self.max_workers:
                self._await_task()
            task = self._task_pool.submit(sub_graph.simple_cycles, seed.next_begin)
        else:
            # Mock Future
            task = Future()
            task.set_result(sub_graph.simple_cycles(seed.next_begin))
        self._running_tasks[seed] = task

    def _run_new_exploration_tasks(self, complete: bool = False):
        self._update_primed_seeds(complete=complete)
        while len(self._primed_seeds) > 0:
            seed = self._primed_seeds.pop()
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

    def _found_cycles(self) -> Iterator[BundledCycle | tuple[_Seed, BundledCycle]]:
        # Get currently done tasks
        self._update_completed_tasks()
        for seed, completed_task in self._completed_tasks.items():
            found_cycles = list(completed_task.result())
            if not self.yield_seeds:
                yield from found_cycles
            else:
                yield from zip(repeat(seed), found_cycles)

    def _get_memory_usage(self) -> int:
        return self._process.memory_info().rss

    def _prune_reverse_reachability(self):
        to_delete: list[Vertex] = []
        # for all summaries S(x) do
        for vertex in self._reverse_reachability:
            reverse_reachability = self._reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.trim_before(self._lower_time_limit)
            if len(reverse_reachability) == 0:
                to_delete.append(vertex)

        for vertex in to_delete:
            del self._reverse_reachability[vertex]

    def _clean_seed_intervals(self):
        to_delete: list[Vertex] = [
            vertex
            for vertex, seed_interval
            in self._seed_intervals.items()
            if seed_interval.is_empty()
        ]
        for vertex in to_delete:
            del self._seed_intervals[vertex]

    def _prune_transaction_graph(self):
        # If there are any primed seeds or running tasks, determine the minimum data_interval begin
        # Keep Track of minimum needed Graph interactions
        interval_begins = []
        # From finished tasks take the latest begin
        if len(self._completed_tasks) > 0:
            interval_begins.append(max(seed.interval.begin for seed in self._completed_tasks))
            self._completed_tasks.clear()

        # From running and future tasks track the earliest start
        interval_begins += [
            seed.interval.begin
            for seed in
            list(self._running_tasks) + self._primed_seeds
        ]

        if len(interval_begins) == 0:
            return

        minimum_graph_begin = min(interval_begins)
        # If the updated minimum sub_graph begin exceeds the transaction sub_graph's begin timestamp,
        if self._transaction_graph.begin() < minimum_graph_begin:
            # prune the sub_graph to remove data older than the new minimum
            self._transaction_graph.prune(lower_time_limit=minimum_graph_begin)

    def cleanup(self):
        self._prune_reverse_reachability()
        self._clean_seed_intervals()
        self._prune_transaction_graph()

    def _get_log_line(
            self,
            header=False
    ) -> list[str | float]:
        current_time = time.monotonic()
        fields = {
            "time_seconds": current_time,
            "iterations_total": self._iteration_count,
            "iterations_rate": self._iteration_count / (current_time - self.start_time),
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

    def __iter__(self) -> Generator[BundledCycle | tuple[_Seed, BundledCycle], None, None]:
        if self.parallel and self._task_pool is None:
            self._initialize_task_pool()

        for edge in self._interactions:
            interaction = Interaction(*edge)
            source, target, timestamp, edge_data = interaction

            # Skip trivial self loops
            if source == target:
                continue

            self._lower_time_limit = interaction.timestamp - self.omega
            self._iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(interaction)  # Process new edge
            self._combine_seeds(interaction.target)  # Merge enclosed intervals
            self._transaction_graph.add_edge(source, target, key=timestamp, **edge_data)

            self._run_new_exploration_tasks()
            yield from self._found_cycles()

            if self._get_memory_usage() > self._max_bytes == 0:
                self.cleanup()

            if self.logging:
                now = monotonic()
                if now - self._last_log_time > self.logging_interval:
                    self._log(log_time=now)

        self._run_new_exploration_tasks(complete=True)
        yield from self._found_cycles()
        if self.parallel:
            self._task_pool.shutdown()
