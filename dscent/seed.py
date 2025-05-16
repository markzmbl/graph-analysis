from __future__ import annotations

from concurrent.futures import Future, wait
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from intervaltree import Interval as DataInterval, IntervalTree
from networkx.classes import MultiDiGraph

from dscent.graph import ExplorationGraph, TransactionGraph, BundledCycle
from dscent.locked_dict import LockedDefaultDict
from dscent.reachability import DirectReachability
from dscent.types_ import Vertex, Timestamp, Interval, Timedelta, TargetInteraction, PointVertices


class Candidates(set[Vertex]):
    """
    A specialized set to track candidates for cycle formation.
    """

    next_begin: Timestamp | None = None  # Start timestamp of the next data_interval, if available


class RootIntervalTree(IntervalTree[Candidates]):
    def merge_enclosed(self, omega):
        """
        Merges intervals in the tree that are enclosed within a distance threshold.

        Two or more intervals are merged if they are close enough (i.e., their
        `begin` times are within `omega` of each other) and if none of them
        individually stretches equal or beyond the `omega` threshold from the starting point
        of the first interval in the group.

        This function modifies the current IntervalTree in place by creating
        a merged list of intervals, but it does not insert them back into `self`.

        Parameters:
            omega (int or float): Maximum allowed distance from the starting interval's
                                  `begin` value to consider merging others.

        Example:
            If omega = 5 and you have intervals:
            [0, 3], [2, 4], [7, 9], [8, 10]
            → it would merge [0, 4] and [7, 10] separately.

        Note:
            - The method assumes that `iv.data` supports the `|=` operator (e.g., `set`).
            - Final merged intervals are stored in `merged`, but not inserted back
              into the tree. You may want to do `self.clear(); self.update(merged)`
              after the call.
        """
        if not self:
            return

        # Sort by start time; longer intervals first when begins are equal
        ivs = sorted(self, key=lambda iv: (iv.begin, -iv.end))
        merged = []

        # Start with the first interval
        iv = ivs.pop(0)
        while ivs:
            begin = iv.begin
            end = iv.end
            data = iv.data
            upper_limit = begin + omega  # Threshold for merging

            # Merge as long as next intervals fall within the upper_limit
            while len(ivs) > 0:
                next_iv = ivs.pop(0)
                if next_iv.end >= upper_limit:
                    # This interval is too far or large; start a new group
                    iv = next_iv
                    break
                # Extend the merged interval
                end = max(end, next_iv.end)
                data |= next_iv.data

            # Store the merged interval
            merged.append(DataInterval(begin, end, data))

        # Add the last remaining interval that wasn’t appended in the loop
        merged.append(iv)

        self.__init__(merged)


@dataclass(frozen=True)
class Seed:
    root: Vertex
    interval: Interval
    candidates: frozenset[Vertex]
    next_begin: Timestamp

    @staticmethod
    def construct(root: Vertex, data_interval: DataInterval[Timestamp, Timestamp, Candidates]):
        begin, end, candidates = data_interval
        candidates.add(root)
        next_begin = candidates.next_begin

        return Seed(
            root=root,
            interval=Interval(begin, end),
            candidates=frozenset(candidates),
            next_begin=next_begin,
        )


class SeedGenerator:
    def __init__(self, omega: Timedelta, thread_pool: ThreadPoolExecutor | None = None):
        # Maximum timestamp window for relevant edges
        self._omega: Timedelta = omega
        # Thread pool for concurrent execution
        self._thread_pool: ThreadPoolExecutor | None = thread_pool
        # Reverse reachability mapping: root -> sorted set of (_lower_limit, predecessor) pairs
        self._reverse_reachability: LockedDefaultDict[Vertex, DirectReachability] = (
            LockedDefaultDict(DirectReachability)
        )
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._root_interval_trees: LockedDefaultDict[Vertex, RootIntervalTree] = (
            LockedDefaultDict(RootIntervalTree)
        )
        self.vertex_timestamps: LockedDefaultDict[Vertex, list[Timestamp]] = LockedDefaultDict(list)
        self._running_tasks: set[Future[None]] = set()

    def _clear_task(self, vertices: list[Vertex], timestamp: Timestamp) -> None:
        """
        Clears the task registry for all running tasks.
        This method is thread-safe and can be called concurrently.
        """
        for vertex in vertices:
            with self.vertex_timestamps.access(vertex) as vertex_timestamps:
                vertex_timestamps.remove(timestamp)
                if len(vertex_timestamps) == 0:
                    del self.vertex_timestamps[vertex]

    def _get_vertex_time(self, vertex: Vertex) -> Timestamp:
        """
        Retrieves the current timestamp for a given vertex.
        This method is thread-safe and can be called concurrently.

        :param vertex: The vertex whose timestamp is to be retrieved.
        :return: The current timestamp for the vertex, or None if not available.
        """
        with self.vertex_timestamps.access(vertex) as vertex_timestamps:
            return min(vertex_timestamps)

    def _seed_generation_task(self, target_interaction: TargetInteraction) -> None:
        """
        Updates reverse reachability and root interval tree
        when a new edge (u -> v) arrives at `current_timestamp`.

        :param target_interaction: (b, A, t)
            where 'A' is the set of source vertices, 'b' is the target vertex, and 't' is the timestamp.
        """
        # (a, b, t) ∈ E
        target = target_interaction.target
        sources = target_interaction.sources
        block_timestamp = target_interaction.timestamp
        lower_limit = block_timestamp - self._omega
        with self._reverse_reachability.access(target) as target_reverse_reachability:
            # Add reachability entry
            # S(b) ← S(b) ∪ {(a, t)}
            target_reverse_reachability.add(PointVertices(vertices=sources, timestamp=block_timestamp))
            # S(b) ← S(b)\{(x, tx) ∈ S(b) | tx ≤ t−ω}
            trimmed_target_reverse_reachability = target_reverse_reachability.get_trimmed_before(lower_limit)
            # Output dict to indicate if new seeds are added
            new_seeds: dict[slice, Candidates] = {}
            for source in sources:
                if source not in self._reverse_reachability:
                    continue
                # if S(a) exists then
                with self._reverse_reachability.access(source) as source_reverse_reachability:
                    # Prune old entries for relevant edges
                    # S(a) ← S(a)\{(x,tx) ∈ S(a) | tx ≤ t−ω}
                    trimmed_source_reverse_reachability = source_reverse_reachability.get_trimmed_before(lower_limit)

                    if len(trimmed_source_reverse_reachability) == 0:
                        del self._reverse_reachability[source]
                        continue

                    # Propagate reachability
                    # S(b) ← S(b) ∪ S(a)
                    trimmed_target_reverse_reachability |= trimmed_source_reverse_reachability
                    # for (b, tb) ∈ S(b) do
                    cyclic_reachability = DirectReachability([
                        v for v in trimmed_target_reverse_reachability
                        if v.vertex == target and v.timestamp < block_timestamp
                    ])
                    # {c ∈ S(a), tc > tb}
                    for cyclic_reachable in cyclic_reachability:
                        # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
                        candidate_reachability = DirectReachability(trimmed_source_reverse_reachability)
                        candidates = Candidates([source])
                        candidates.update(c.vertex for c in candidate_reachability)
                        if len(candidates) > 1:
                            # Output (b, [tb, t], C)
                            seed_range = slice(cyclic_reachable.timestamp, block_timestamp)
                            new_seeds[seed_range] = candidates
                    # trim source reachability
                    source_reverse_reachability.trim_before(lower_limit=self._get_vertex_time(source))

            # Remove to avoid duplicate output
            # S(b) ← S(b) \ {(b, tb)}
            # And trim the reachability set
            target_time = self._get_vertex_time(vertex=target)
            self._reverse_reachability[target] = DirectReachability(
                reverse_reachable
                for reverse_reachable in trimmed_target_reverse_reachability
                if reverse_reachable.vertex != target and reverse_reachable.timestamp >= target_time
            )

        # Exit the reverse reachability locks and consider new seeds in target root interval tree
        if new_seeds:
            # New candidate seeds are present
            with self._root_interval_trees.access(target) as root_interval_tree:  # Enter root interval tree lock
                for seed_range, candidates in new_seeds.items():
                    # Add new seeds
                    root_interval_tree[seed_range] = candidates
                # Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
                if len(root_interval_tree) > 1:
                    root_interval_tree.merge_enclosed(self._omega)

    def submit(self, interaction: TargetInteraction) -> None:
        """
        Submits a new target interaction for processing.
        This method is thread-safe and can be called concurrently.

        :param target_interaction: A named tuple containing the target vertex, source vertices, and timestamp.
        """
        timestamp = interaction.timestamp
        vertices = [interaction.target] + interaction.sources
        # and add the task to the running task registry.
        for vertex in vertices:
            with self.vertex_timestamps.access(vertex) as vertex_timestamps:
                # Add the timestamp to the list of running tasks for the vertex
                vertex_timestamps.append(timestamp)
        # Submit the seed generation task to the thread pool
        if self._thread_pool is not None:
            future = self._thread_pool.submit(self._seed_generation_task, interaction)
        else:
            future = Future()
            future.set_result(self._seed_generation_task(interaction))
        future.add_done_callback(lambda _: self._clear_task(vertices, timestamp))
        self._running_tasks.add(future)

    def pop_primed_seeds(self, upper_limit: Timestamp | None = None) -> list[Seed]:
        """
        Retrieve, pop and return primed seeds from the root interval trees.

        :param upper_limit: latest timestamp to consider for primed seeds.
        :return:
        """
        # Initialize a list to store primed seeds
        primed_seeds: list[Seed] = []
        # Iterate over each root and its associated interval tree
        for root in list(self._root_interval_trees.keys()):
            with self._root_interval_trees.access(root) as interval_tree:
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

    def _prune_reverse_reachability(self, current_time: Timestamp) -> None:
        for vertex in list(self._reverse_reachability.keys()):
            with (
                self._reverse_reachability.access(vertex) as direct_reachability,
                self.vertex_timestamps.access(vertex) as vertex_timestamps
            ):
                if len(direct_reachability) > 0:
                    # Get the earliest relevant timestamp
                    vertex_lower_limit = min([current_time] + vertex_timestamps) - self._omega
                    # Prune the reachability set
                    direct_reachability.trim_before(vertex_lower_limit)
                # Check (afterward) if the reverse reachability set is empty
                if len(direct_reachability) == 0:
                    del self._reverse_reachability[vertex]

    def wait(self) -> None:
        """
        Wait for all running tasks to complete.
        """
        wait(self._running_tasks)

    def cleanup(self, current_time: Timestamp):
        for task in list(self._running_tasks):
            if task.done():
                self._running_tasks.remove(task)
        self._prune_reverse_reachability(current_time=current_time)


class SeedExplorer:
    def __init__(self, omega: Timedelta, thread_pool: ThreadPoolExecutor | None = None):
        # Maximum timestamp window for relevant edges
        self._omega: Timedelta = omega
        # Thread pool for concurrent execution
        self._thread_pool: ThreadPoolExecutor | None = thread_pool
        # Dynamic directed sub_graph
        self._transaction_graph = TransactionGraph()
        self._running_tasks: dict[Seed, Future[list[BundledCycle]]] = {}
        self._explored_seeds: set[Seed] = set()

    def add_edge(self, source: Vertex, target: Vertex, timestamp: Timestamp, **edge_data: dict[str, Any]) -> None:
        # Add edge to the transaction graph
        self._transaction_graph.add_edge(source, target, key=timestamp, **edge_data)

    def _explore_seed(self, expoloration_graph: ExplorationGraph, next_seed_begin: Timestamp) -> list[BundledCycle]:
        return expoloration_graph.simple_cycles(next_seed_begin)

    def submit(self, primed_seed: Seed) -> None:
        """
        Submits a new primed seed for exploration.
        This method is thread-safe and can be called concurrently.

        :param primed_seed: A Seed object containing the root vertex, interval, candidates, and next_begin timestamp.
        """
        # Seed relevant Subgraph View
        graph_view = self._transaction_graph.time_slice(
            begin=primed_seed.interval.begin,
            end=primed_seed.interval.end,
            closed=True,
            nodes=primed_seed.candidates
        )
        # Root Vertex
        root_vertex = primed_seed.root
        # Make Copy
        sub_graph = ExplorationGraph(graph_view, root_vertex=root_vertex)
        next_seed_begin = (
            primed_seed.next_begin
            if primed_seed.next_begin is not None
            else primed_seed.interval.begin + self._omega
        )
        if self._thread_pool is not None:
            self._running_tasks[primed_seed] = self._thread_pool.submit(
                self._explore_seed, sub_graph, next_seed_begin
            )
        else:
            future = Future()
            future.set_result(self._explore_seed(sub_graph, next_seed_begin))
            self._running_tasks[primed_seed] = future

    def pop_detected_cycle_graphs(
            self, include_seeds: bool = False
    ) -> list[BundledCycle] | dict[Seed, list[BundledCycle]]:
        """
        Retrieve and pop explored graphs from the running tasks.

        :param include_seeds: Yields seeds from the root interval tree.
        :return: A list of seeds that have been explored.
        """
        detected_graphs = {}
        for seed, future in list(self._running_tasks.items()):
            if future.done():
                cycle_graphs = future.result()
                detected_graphs[seed] = cycle_graphs
                del self._running_tasks[seed]
                self._explored_seeds.add(seed)
        if include_seeds:
            return detected_graphs
        else:
            return [
                graph for cycle_graphs in detected_graphs.values()
                for graph in cycle_graphs
            ]

    def _prune_transaction_graph(self):
        # If there are any primed seeds or running tasks, determine the minimum data_interval begin
        # Keep Track of minimum needed Graph interactions
        thresholds = []
        # From finished tasks take the latest begin
        if len(self._explored_seeds) > 0:
            thresholds.append(max(seed.interval.begin for seed in self._explored_seeds))
            self._explored_seeds.clear()  # Clear explored seeds
        # From running tasks track the earliest start
        thresholds += [seed.interval.begin for seed in self._running_tasks]
        # No running tasks
        if len(thresholds) == 0:
            return

        minimum_graph_begin = min(thresholds)
        # If the updated minimum sub_graph begin exceeds the transaction sub_graph's begin timestamp,
        if self._transaction_graph.begin() < minimum_graph_begin:
            # prune the sub_graph to remove data older than the new minimum
            self._transaction_graph.prune(lower_time_limit=minimum_graph_begin)

    def wait(self):
        """
        Wait for all running tasks to complete.
        :return:
        """
        wait(self._running_tasks.values())

    def cleanup(self):
        self._prune_transaction_graph()
