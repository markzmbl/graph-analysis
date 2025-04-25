from __future__ import annotations

import contextlib
import csv
import io
import time
from collections.abc import Iterator, Generator
from itertools import repeat
from time import monotonic
from typing import TextIO

import psutil
from humanfriendly import parse_size
from tqdm import tqdm

from dscent.graph import TransactionGraph, ExplorationGraph, BundledCycle
from dscent.locked_defaultdict import LockedDefaultDict
from dscent.reachability import DirectReachability
from dscent.seed import Seed, RootIntervalTree, Candidates
from dscent.task_registry import TaskType, TaskRegistry
from dscent.types_ import TimeDelta, Interaction, Vertex, SingleTimedVertex, Timestamp


class GraphCycleIterator:
    """
    Implements a core data structure and methods inspired by the 2SCENT algorithm.
    """

    def __init__(
            self,
            interactions: Iterator[Interaction],
            omega: TimeDelta = 10,
            max_workers: int = 2,
            queue_size: int = 10_000,
            garbage_collection_max: int | str = "32G",
            garbage_collection_cooldown: int = 10_000_000,
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
        self._current_time = 0
        # Parallel setup
        self._max_workers = max_workers  # Maximum number of active workers
        self._task_registry = TaskRegistry(max_workers=max_workers, queue_size=queue_size)
        self._explored_seeds: set[Seed] = set()
        # Time Threshold
        self._omega = omega
        # Reverse reachability mapping: root -> sorted set of (_lower_limit, predecessor) pairs
        self._reverse_reachability: LockedDefaultDict[Vertex, DirectReachability] = (
            LockedDefaultDict(DirectReachability)
        )
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._root_interval_trees: LockedDefaultDict[Vertex, RootIntervalTree] = (
            LockedDefaultDict(RootIntervalTree)
        )
        # Memory Monitoring
        self._process = psutil.Process()
        self._max_bytes = parse_size(garbage_collection_max)  # max bytes for pruning old data
        self._last_cleaned = float("-inf")
        self._cleanup_cooldown = garbage_collection_cooldown
        # Seeds
        self._primed_seeds: list[Seed] = []
        self._yield_seeds = yield_seeds
        # Dynamic directed sub_graph
        self._transaction_graph = TransactionGraph()
        # Logging
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

    @contextlib.contextmanager
    def _enter_reachability(self, key: Vertex):
        with self._reverse_reachability.enter_lock(key):
            yield

    @contextlib.contextmanager
    def _enter_root_interval_tree(self, key: Vertex):
        with self._root_interval_trees.enter_lock(key):
            yield

    def _get_trimmed_reachability(self, vertex: Vertex, lower_limit: Timestamp) -> DirectReachability:
        return self._reverse_reachability[vertex].get_trimmed_before(lower_limit=lower_limit, strict=False)

    def _trim_reachability(self, vertex: Vertex, lower_limit: Timestamp) -> None:
        vertex_time = self._get_current_vertex_time(vertex=vertex)
        self._reverse_reachability[vertex].trim_before(
            lower_limit=vertex_time if vertex_time is not None else lower_limit,
            strict=False
        )

    def _generate_seeds(self, interaction: Interaction) -> None:
        """
        Updates reverse reachability and root interval tree
        when a new edge (u -> v) arrives at `current_timestamp`.

        :param interaction: (a, b, t)
            where 'a' is the source vertex, 'b' is the target vertex, and 't' is the timestamp.
        """
        lower_limit = interaction.timestamp - self._omega
        # (a, b, t) ∈ E
        source, target, current_timestamp = interaction
        with self._enter_reachability(target):
            target_reverse_reachability = self._reverse_reachability[target]
            # Add reachability entry
            # S(b) ← S(b) ∪ {(a, t)}
            target_reverse_reachability.append(SingleTimedVertex(
                vertex=source, timestamp=current_timestamp
            ))
            # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
            target_reverse_reachability = self._get_trimmed_reachability(target, lower_limit)
            if source not in self._reverse_reachability:
                self._trim_reachability(target, lower_limit)
                return
            # if S(a) exists then
            with self._enter_reachability(source):
                # Prune old entries for relevant edges
                # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
                source_reverse_reachability = self._get_trimmed_reachability(source, lower_limit)
                # Output dict to indicate if new seeds are added
                new_seeds: dict[slice, Candidates] = {}
                if len(source_reverse_reachability) == 0:
                    del self._reverse_reachability[source]
                    return
                # Propagate reachability
                # S(b) ← S(b) ∪ S(a)
                target_reverse_reachability |= source_reverse_reachability
                # for (b, tb) ∈ S(b) do
                cyclic_reachability = DirectReachability([
                    v for v in target_reverse_reachability
                    if v.vertex == target and v.timestamp < current_timestamp
                ])
                if len(cyclic_reachability) == 0:
                    return
                # {c ∈ S(a), tc > tb}
                for cyclic_reachable in cyclic_reachability:
                    # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
                    candidate_reachability = DirectReachability(source_reverse_reachability)
                    candidates = Candidates([source])
                    candidates.update(c.vertex for c in candidate_reachability)
                    if len(candidates) > 1:
                        # Output (b, [tb, t], C)
                        seed_range = slice(cyclic_reachable.timestamp, current_timestamp)
                        new_seeds[seed_range] = candidates
                # Remove to avoid duplicate output
                # S(b) ← S(b) \ {(b, tb)}
                self._reverse_reachability[target] = DirectReachability(
                    reverse_reachable
                    for reverse_reachable in target_reverse_reachability
                    if reverse_reachable.vertex != target
                )
                # Trim inplace
                self._trim_reachability(target, lower_limit)
                self._trim_reachability(source, lower_limit)

        # Exit the reverse reachability locks and consider new seeds in target root interval tree
        if new_seeds:
            # New candidate seeds are present
            with self._enter_root_interval_tree(target):  # Enter root interval tree lock
                root_interval_tree = self._root_interval_trees[target]
                for seed_range, candidates in new_seeds.items():
                    # Add new seeds
                    root_interval_tree[seed_range] = candidates
                # Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
                if len(root_interval_tree) > 1:
                    root_interval_tree.merge_enclosed(self._omega)

    def _get_primed_seeds(self, upper_limit: Timestamp | None = None):
        # Initialize a list to store primed seeds
        primed_seeds: list[Seed] = []
        # Iterate over each root and its associated interval tree
        for root, interval_tree in list(self._root_interval_trees.items()):
            with self._enter_root_interval_tree(root):
                # Check if the interval tree contains intervals starting before or at the cutoff time
                primed_intervals = []
                all_intervals = False
                # There is no upper limit, collect all intervals of all interval trees
                if upper_limit is None:
                    primed_intervals = list(interval_tree)
                    all_intervals = True
                # Interval tree lies before upper limit, collect whole interval tree
                elif interval_tree.end() <= upper_limit:
                    primed_intervals = list(interval_tree)
                    all_intervals = True
                # Upper Limit lies within the interval tree, collect envelope of interval tree
                elif interval_tree.begin() < upper_limit <= interval_tree.end():
                    primed_intervals = interval_tree.envelop(begin=interval_tree.begin(), end=upper_limit)
                # Convert each qualifying interval into a Seed object and collect them
                for primed_interval in primed_intervals:
                    primed_seeds.append(Seed.construct(root=root, data_interval=primed_interval))
                    if not all_intervals:
                        interval_tree.remove(primed_interval)
                # If all intervals have been processed, clear the entire tree
                if all_intervals:
                    interval_tree.clear()
                # Remove the root from the tree dictionary if its interval tree is now empty
                if len(interval_tree) == 0:
                    del self._root_interval_trees[root]
        # Return the list of gathered seeds
        return primed_seeds

    def _submit_seed_generation_task(self, interaction: Interaction):
        self._task_registry.submit(
            task_type=TaskType.SEED_GENERATION,
            task_key=interaction,
            fn=self._generate_seeds,
            interaction=interaction,
        )

    def _submit_exploration_task(self, sub_graph: ExplorationGraph, seed: Seed):
        next_seed_begin = (
            seed.next_begin
            if seed.next_begin is not None
            else seed.interval.begin + self._omega
        )
        self._task_registry.submit(
            task_type=TaskType.EXPLORATION,
            task_key=seed,
            persist=True,
            fn=sub_graph.simple_cycles,
            next_seed_begin=next_seed_begin,
        )

    def _get_current_vertex_time(self, vertex: Vertex) -> Timestamp | None:
        # Vertex specific timestamps
        return min(
            (
                interaction.timestamp for interaction
                in self._task_registry.get_running_seed_generation_tasks(vertex)
            ),
            default=None
        )

    def _get_effective_current_timestamp(self) -> Timestamp:
        # All running timestamps
        timestamps = [self._current_time]
        timestamps += [
            interaction.timestamp for interaction in
            self._task_registry.get_running_tasks(task_type=TaskType.SEED_GENERATION)
        ]
        return min(timestamps)

    def _run_new_exploration_tasks(self, complete=False):
        upper_limit = self._get_effective_current_timestamp() - self._omega if not complete else None
        for seed in self._get_primed_seeds(upper_limit=upper_limit):
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

    def _found_cycles(self) -> Iterator[BundledCycle | tuple[Seed, BundledCycle]]:
        # Get currently done tasks
        self._task_registry.update_running_tasks(task_type=TaskType.EXPLORATION)
        completed_exploration_tasks = self._task_registry.get_completed_tasks(task_type=TaskType.EXPLORATION)
        for seed, completed_task in completed_exploration_tasks.items():
            found_cycles = list(completed_task.result())
            if not self._yield_seeds:
                yield from found_cycles
            else:
                yield from zip(repeat(seed), found_cycles)
        # The yielded seeds are added to the collection of explored seeds
        self._explored_seeds.update(completed_exploration_tasks)
        # And removed from the exploration task registry
        self._task_registry.clear_completed_tasks(task_type=TaskType.EXPLORATION)

    def _get_memory_usage(self) -> int:
        return self._process.memory_info().rss

    def _prune_reverse_reachability(self):
        lower_limit = self._current_time - self._omega
        for vertex, direct_reachability in list(self._reverse_reachability.items()):
            if len(direct_reachability) > 0:
                vertex_time = self._get_current_vertex_time(vertex)
                self._trim_reachability(
                    vertex,
                    lower_limit=(
                        vertex_time
                        if vertex_time is not None
                        else lower_limit
                    )
                )
            if len(direct_reachability) == 0:
                del self._reverse_reachability[vertex]

    def _prune_transaction_graph(self):
        # If there are any primed seeds or running tasks, determine the minimum data_interval begin
        # Keep Track of minimum needed Graph interactions
        thresholds = []
        # From finished tasks take the latest begin
        if len(self._explored_seeds) > 0:
            thresholds.append(max(seed.interval.begin for seed in self._explored_seeds))
            self._explored_seeds.clear()  # Clear explored seeds
        # From running tasks track the earliest start
        thresholds += [
            seed.interval.begin
            for seed in
            list(self._task_registry.get_running_tasks(task_type=TaskType.EXPLORATION))
        ]
        # No running tasks
        if len(thresholds) == 0:
            return

        minimum_graph_begin = min(thresholds)
        # If the updated minimum sub_graph begin exceeds the transaction sub_graph's begin timestamp,
        if self._transaction_graph.begin() < minimum_graph_begin:
            # prune the sub_graph to remove data older than the new minimum
            self._transaction_graph.prune(lower_time_limit=minimum_graph_begin)

    def _memory_limit_exceeded(self) -> bool:
        """
        Check if the memory limit is exceeded.
        """
        return self._get_memory_usage() > self._max_bytes

    def _cleanup_cooled_down(self) -> bool:
        """
        Check if the cleanup cooldown period has passed.
        """
        return self._iteration_count > self._last_cleaned + self._cleanup_cooldown

    def cleanup(self):
        self._prune_reverse_reachability()
        # self._prune_transaction_graph()
        self._last_cleaned = self._iteration_count

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
        for edge in self._interactions:
            source, target, current_time, edge_data = edge
            self._current_time = current_time
            interaction = Interaction(source, target, current_time)
            # Skip trivial self loops
            if source == target:
                continue
            # Track iteration count
            self._iteration_count += 1
            # Submit Seed Generation Task
            self._submit_seed_generation_task(interaction)
            # Add Edge to transaction graph
            self._transaction_graph.add_edge(source, target, key=current_time, **edge_data)
            # Try to gather new exploration tasks
            self._run_new_exploration_tasks()
            # Yield results from finished explorations
            yield from self._found_cycles()
            # Check if memory is exceeded
            if self._memory_limit_exceeded() and self._cleanup_cooled_down():
                # Cleanup Memory
                self.cleanup()
            # Logging
            if self._logging:
                now = monotonic()
                # Check if Logging interval is exceeded
                if now - self._last_log_time > self._logging_interval:
                    self._log(log_time=now)
        # Wait for final seed generation tasks to finish
        self._task_registry.wait(await_all=True)
        # Start final exploration tasks
        self._run_new_exploration_tasks(complete=True)
        # Wait for final exploration tasks to finish
        self._task_registry.wait(await_all=True)
        # Yield final found cycles
        yield from self._found_cycles()
        self._task_registry.shutdown()
