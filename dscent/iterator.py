from __future__ import annotations

import io
import time
from collections import defaultdict, Counter
from collections.abc import Iterator, Hashable
from concurrent.futures import Future, wait, FIRST_COMPLETED
from concurrent.futures.thread import ThreadPoolExecutor
from tempfile import TemporaryFile, NamedTemporaryFile
from threading import RLock, Lock, Event
from time import sleep

import numpy as np
import psutil
from intervaltree import IntervalTree, Interval
from pandas import Timestamp
from tqdm.auto import tqdm

from dscent.graph import TransactionGraph
from dscent.types import Vertex, TimeDelta, Interaction, ReverseReachableVertex, ReverseReachabilitySet, \
    Candidates, Seed

MY_LOCK = RLock()


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            interactions: Iterator[Interaction],
            omega: TimeDelta = 10,
            max_workers: int = 1,
            cleanup_interval: int = 1_000,
            log_stream: io.StringIO | None = None,
            logging_interval: int = 60
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum time
        window, and a prune interval.
        """
        self._interactions = iter(interactions)  # Stream of edges
        self._iteration_count: int = 0  # Track number of processed edges

        self.cleanup_interval: int = cleanup_interval  # Interval for pruning old data
        self.omega: TimeDelta = omega  # Maximum time window for relevant edges

        self._thread_pool: ThreadPoolExecutor | None = None
        self._pickup_lock: Lock | None = None
        self._finalize_lock: Lock | None = None
        self.max_workers = max_workers  # Maximum number of active workers
        self._running_tasks: dict[Timestamp, Future] = {}
        self._completed_tasks: dict[Timestamp, Future] = {}

        # Reverse reachability mapping: vertex -> sorted set of (_lower_time_limit, predecessor) pairs
        self._reverse_reachability: dict[Vertex, ReverseReachabilitySet] = defaultdict(ReverseReachabilitySet)

        # Interval tracking for candidate cycles: vertex -> IntervalTree
        self._seed_intervals: dict[Vertex, IntervalTree] = defaultdict(IntervalTree)

        # Seeds which can not grow any further and are primed for exploration
        self._primed_seeds: list[Seed] = []
        self._minimum_graph_begin = 0

        # Dynamic directed graph with edge removal enabled
        self._transaction_graph = TransactionGraph()

        self.start_time = time.monotonic()
        if log_stream is None:
            self.logging = False
        else:
            self.logging = True
            self.logging_interval = logging_interval
            self._log_stream = log_stream
            self._process = psutil.Process()
            self._write_log_header()
            self._last_log_time = self.start_time

    def _initialize_thread_pool(self):
        self._thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)
        self._pickup_lock = Lock()
        self._finalize_lock = Lock()

    def _update_reverse_reachability(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability when a new edge (u -> v) arrives at `current_timestamp`.
        """
        # (a, b, t) ∈ E
        source, target, current_timestamp = (
            interaction.source,
            interaction.target,
            interaction.timestamp,
        )
        target_reverse_reachability = self._reverse_reachability[target]

        # Add reachability entry
        # S(b) ← S(b) ∪ {(a, t)}
        target_reverse_reachability.append(ReverseReachableVertex(
            vertex=source, timestamp=current_timestamp
        ))

        # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
        target_reverse_reachability.prune(self._lower_time_limit)

        if source not in self._reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self._reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.prune(self._lower_time_limit)

        # Propagate reachability
        # S(b) ← S(b) ∪ S(a)
        target_reverse_reachability |= source_reverse_reachability

        # for (b, tb) ∈ S(b) do
        cyclic_reachability = ReverseReachabilitySet(
            v for v in target_reverse_reachability if v.vertex == target
        )
        if len(cyclic_reachability) == 0:
            return

        # {c ∈ S(a), tc > tb}
        for cyclic_reachable in cyclic_reachability:
            candidate_reachability = ReverseReachabilitySet(source_reverse_reachability)

            candidate_reachability.prune(
                lower_time_limit=cyclic_reachable.timestamp,
                strictly_smaller=True
            )

            if len(candidate_reachability) == 0:
                continue

            # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
            candidates = Candidates([source])
            candidates.update(c.vertex for c in candidate_reachability)

            if len(candidates) > 1:
                candidates.next_begin = cyclic_reachable.timestamp + self.omega
                # Output (b, [tb, t], C)
                self._seed_intervals[target][cyclic_reachable.timestamp: current_timestamp] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = ReverseReachabilitySet(
            v for v in target_reverse_reachability if v not in cyclic_reachability
        )

    def _combine_seeds(self, v: Hashable) -> None:
        """
        Merges adjacent or overlapping intervals for vertex `v` within the allowed time window.
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
                combined_candidates = Candidates(compatible_interval.data)
                combined_interval_end = compatible_interval.end
            else:
                combined_candidates = Candidates()
                combined_interval_end = -np.inf

                for compatible_interval in compatible_intervals:
                    combined_candidates.update(compatible_interval.data)
                    combined_interval_end = max(combined_interval_end, compatible_interval.end)  # Extend interval

            interval_tree.remove_envelop(first_interval_begin, upper_interval_limit)  # Remove merged intervals
            if interval_tree:
                combined_candidates.next_begin = interval_tree.begin()  # set next interval start
            else:
                combined_candidates.next_begin = upper_interval_limit  # No more intervals

            # Store merged interval
            combined_interval_tree[first_interval_begin: combined_interval_end] = combined_candidates

        if combined_interval_tree:
            self._seed_intervals[v] = combined_interval_tree  # Update _seed_intervals

    def _update_primed_seeds(self):
        lower_time_range = (0, self._lower_time_limit)
        for vertex, interval_tree in self._seed_intervals.items():
            for interval in interval_tree.envelop(*lower_time_range):
                seed = Seed(
                    vertex=vertex,
                    begin=interval.begin,
                    end=interval.end,
                    candidates=interval.data,
                )
                self._primed_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)

    def _constrained_depth_first_search(self, seed: Seed):
        seed.candidates.add(seed.vertex)

        exploration_graph = (
            self._transaction_graph
            .time_slice(seed.begin, seed.end)
            .subgraph(seed.candidates)
        )
        # TODO: add logic for pairs u <-> v
        with self._finalize_lock:
            if self.logging:
                self._log(
                    exploration_interval=Interval(seed.begin, seed.end),
                    exploration_graph=exploration_graph
                )

        return seed.begin

    def _submit_exploration_task(self, seed: Seed):
        while len(self._running_tasks) >= self.max_workers:
            done_tasks, _ = wait(self._running_tasks.values(), return_when=FIRST_COMPLETED)
            for seed_begin, task in list(self._running_tasks.items()):
                if task in done_tasks:
                    self._completed_tasks[seed_begin] = self._running_tasks.pop(seed_begin)
        task = self._thread_pool.submit(self._constrained_depth_first_search, seed)
        self._running_tasks[seed.begin] = task

    def _run_new_exploration_tasks(self):
        while len(self._primed_seeds) > 0:
            exploration_seed = self._primed_seeds.pop()
            self._submit_exploration_task(seed=exploration_seed)
            # self._constrained_depth_first_search(exploration_seed)

    def cleanup(self):
        # for all summaries S(x) do
        for vertex in list(self._reverse_reachability):
            reverse_reachability = self._reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.prune(self._lower_time_limit)
            if len(reverse_reachability) == 0:
                del self._reverse_reachability[vertex]

        for vertex in list(self._seed_intervals):
            if self._seed_intervals[vertex].is_empty():
                del self._seed_intervals[vertex]


        # If there are any primed seeds or running tasks, determine the minimum interval begin
        if len(self._primed_seeds) > 0 or len(self._running_tasks) > 0:
            seed_interval_begins = [seed.begin for seed in self._primed_seeds] + list(self._running_tasks.keys())

            current_seed_interval_minimum = (
                min(seed_interval_begins)
                if len(seed_interval_begins) > 1
                else seed_interval_begins[0]
            )

            # Update the minimum graph begin time if the new value is greater
            if current_seed_interval_minimum > self._minimum_graph_begin:
                self._minimum_graph_begin = current_seed_interval_minimum

                # If the updated minimum graph begin exceeds the transaction graph's begin time,
                # prune the graph to remove data older than the new minimum
                if self._minimum_graph_begin > self._transaction_graph.begin():
                    self._transaction_graph.prune(lower_time_limit=self._minimum_graph_begin)

    def _get_log_line(
            self,
            interval: Interval | None = None,
            graph: TransactionGraph | None = None,
            header=False
    ) -> list[str | float]:
        current_time = time.monotonic()
        fields = {
            "time_seconds": current_time,
            "iterations_total": self._iteration_count,
            "iterations_rate": self._iteration_count / (current_time - self.start_time),
            "memory_usage_bytes": self._process.memory_info().rss,
            "seed_interval_length": None,
            "interval_length_delta": None,
            "number_of_nodes": None,
            "number_of_edges": None,
        }

        if header:
            return list(fields.keys())
        elif graph and interval:
            interval_length = interval.length()
            graph_length = graph.length()
            fields.update({
                "seed_interval_length": interval_length,
                "interval_length_delta": abs(interval_length - graph_length),
                "number_of_nodes": graph.number_of_nodes(),
                "number_of_edges": graph.number_of_edges(),
            })
        return list(fields.values())

    @staticmethod
    def _format_log_line(values: list[str]) -> str:
        return ",".join(str(value) for value in values) + "\n"

    def _write_log_header(self):
        header_values = self._get_log_line(header=True)
        header_line = self._format_log_line(header_values)
        self._log_stream.write(header_line)

    def _log(
            self,
            exploration_interval: Interval | None = None,
            exploration_graph: TransactionGraph | None = None,
    ):
        log_line_values = self._get_log_line(interval=exploration_interval, graph=exploration_graph)
        log_line = self._format_log_line(log_line_values)
        self._log_stream.write(log_line)

    def __iter__(self) -> "GraphCycleIterator":
        """
        Allows the GraphCycleIterator to be used as an iterator in a for-loop.
        """
        return self

    def __next__(self):
        if self._thread_pool is None:
            self._initialize_thread_pool()

        for source, target, timestamp in self._interactions:
            # Skip trivial self loops
            if source == target:
                continue

            interaction = Interaction(source, target, timestamp)
            self._lower_time_limit = interaction.timestamp - self.omega

            self._iteration_count += 1  # Track iteration count

            self._update_reverse_reachability(interaction)  # Process new edge

            self._combine_seeds(interaction.target)  # Merge enclosed intervals

            self._update_primed_seeds()

            self._transaction_graph.add_edge(interaction.source, interaction.target, key=interaction.timestamp)

            self._run_new_exploration_tasks()

            if self._iteration_count % self.cleanup_interval == 0:
                self.cleanup()

            # TODO: actually yield cycles from future result
            self._completed_tasks = {}

            if (self._last_log_time - self.start_time) > self.logging_interval:
                self._log()

        wait(self._running_tasks.values())
        self._thread_pool.shutdown()
        self._log_stream.flush()
        raise StopIteration()
