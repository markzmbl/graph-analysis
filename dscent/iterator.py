from __future__ import annotations

import io
import time
from collections import defaultdict
from collections.abc import Iterator, Hashable
from concurrent.futures import Future, wait, FIRST_COMPLETED
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass

import psutil
from intervaltree import IntervalTree, Interval
from pandas import Timestamp

from dscent.graph import TransactionGraph, ExplorationGraph
from dscent.types_ import (
    Vertex, TimeDelta, Interaction, SingleTimedVertex, Timestamp
)
from dscent.reachability import DirectReachability


class _Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next interval, if available


@dataclass(frozen=True)
class _Seed:
    root: Vertex
    interval: Interval
    candidates: frozenset[Vertex]
    next_begin: Timestamp

    @staticmethod
    def construct(root: Vertex, interval: Interval[Timestamp, Timestamp, _Candidates]):
        candidates = interval.data
        interval.data.add(root)
        next_begin = candidates.next_begin
        return _Seed(
            root=root,
            interval=interval,
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
            max_workers: int = 0,
            threaded: bool = False,
            # cleanup_interval: int = 1_000_000,
            cleanup_interval: int = 1_000,
            log_stream: io.StringIO | None = None,
            logging_interval: int = 60
    ) -> None:
        """
        Initializes the GraphCycleIterator with streaming edges, a maximum timestamp
        window, and a prune interval.
        """
        self._interactions = iter(interactions)  # Stream of edges
        self._iteration_count: int = 0  # Track number of processed edges

        self.cleanup_interval: int = cleanup_interval  # Interval for pruning old data
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

        # Dynamic directed sub_graph with edge removal enabled
        self._transaction_graph = TransactionGraph()

        if log_stream is None:
            self.logging = False
            self.start_time = None
        else:
            self.start_time = time.monotonic()
            self.logging = True
            self.logging_interval = logging_interval
            self._log_stream = log_stream
            self._process = psutil.Process()
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
        target_reverse_reachability.trim_before(self._lower_time_limit)

        if source not in self._reverse_reachability:
            return

        # if S(a) exists then
        source_reverse_reachability = self._reverse_reachability[source]

        # Prune old entries for relevant edges
        # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
        source_reverse_reachability.trim_before(self._lower_time_limit)

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
                seed = _Seed.construct(root=vertex, interval=interval)
                self._primed_seeds.append(seed)
            interval_tree.remove_envelop(*lower_time_range)

    def _update_completed_tasks(self):
        for seed, running_task in list(self._running_tasks.items()):
            if running_task.done():
                # Pop from _running_tasks, put into _completed_tasks
                done_task = self._running_tasks.pop(seed)
                self._completed_tasks[seed] = done_task

    def _submit_exploration_task(self, sub_graph: ExplorationGraph, seed: _Seed):
        if self.parallel:
            # Wait if necessary
            while len(self._running_tasks) >= self.max_workers:
                wait(self._running_tasks.values(), return_when=FIRST_COMPLETED)
            task = self._task_pool.submit(sub_graph.simple_cycles, seed.next_begin)
        else:
            # Mock Future
            task = Future()
            task.set_result(sub_graph.simple_cycles(seed.next_begin))
        self._running_tasks[seed] = task

    def _run_new_exploration_tasks(self):
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


    def _found_cycles(self):
        # Get currently done tasks
        self._update_completed_tasks()
        for completed_task in self._completed_tasks.values():
            found_cycles = list(completed_task.result())
            yield from found_cycles

    def cleanup(self):
        # for all summaries S(x) do
        for vertex in list(self._reverse_reachability):
            reverse_reachability = self._reverse_reachability[vertex]
            # S(x) ← S(x)\{(y,ty) ∈ S(x) | ty ≤ t−ω}
            reverse_reachability.trim_before(self._lower_time_limit)
            if len(reverse_reachability) == 0:
                del self._reverse_reachability[vertex]

        for vertex in list(self._seed_intervals):
            if self._seed_intervals[vertex].is_empty():
                del self._seed_intervals[vertex]

        # If there are any primed seeds or running tasks, determine the minimum interval begin
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
        if len(interval_begins):
            minimum_graph_begin = min(interval_begins)

            if self._transaction_graph.begin() < minimum_graph_begin:
                # If the updated minimum sub_graph begin exceeds the transaction sub_graph's begin timestamp,
                # prune the sub_graph to remove data older than the new minimum
                self._transaction_graph.prune(lower_time_limit=minimum_graph_begin)

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

    def __iter__(self):
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
            self._update_primed_seeds()

            self._transaction_graph.add_edge(source, target, key=timestamp, **edge_data)
            if self.parallel:
                self._run_new_exploration_tasks()
            yield from self._found_cycles()

            if self._iteration_count % self.cleanup_interval == 0:
                self.cleanup()

            if self.logging and (self._last_log_time - self.start_time) > self.logging_interval:
                self._log()

        self._lower_time_limit = float("inf")
        self._update_primed_seeds()
        self._run_new_exploration_tasks()
        wait(self._running_tasks.values())
        yield from self._found_cycles()
        if self._task_pool is not None:
            self._task_pool.shutdown()

        if self.logging:
            self._log_stream.flush()
