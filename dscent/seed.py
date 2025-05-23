from __future__ import annotations

import os
from collections import defaultdict
from concurrent.futures import Future, wait
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from functools import reduce
from itertools import repeat
from typing import Any

from intervaltree import Interval as DataInterval, IntervalTree
from networkx.classes import MultiDiGraph

from dscent.graph import ExplorationGraph, TransactionGraph, BundledCycle
from dscent.locked_dict import LockedDefaultDict
from dscent.reachability import DirectReachability
from dscent.types_ import Vertex, Timestamp, Interval, Timedelta, TargetInteraction, PointVertices, TransactionBlock, \
    PointVertex


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
    def __init__(self, omega: Timedelta):
        # Maximum timestamp window for relevant edges
        self._omega: Timedelta = omega
        # Thread pool for concurrent execution
        # self._thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1024)
        # Reverse reachability mapping: root -> sorted set of (_lower_limit, predecessor) pairs
        self._reverse_reachability: defaultdict[Vertex, DirectReachability] = (
            defaultdict(DirectReachability)
        )
        # Interval tracking for candidate cycles: root -> IntervalTree
        self._root_interval_trees: defaultdict[Vertex, RootIntervalTree] = (
            defaultdict(RootIntervalTree)
        )

    def _prune_reverse_reachability(self, vertex: Vertex, lower_limit: Timedelta) -> None:
        # If a reverse reachability for an interacting vertex exists
        if vertex in self._reverse_reachability:
            reverse_reachability = self._reverse_reachability[vertex]
            if len(reverse_reachability) > 0:
                # It shall be pruned and trimmed before the lower limit
                # S(v) ← S(v)\{(x, tx) ∈ S(v) | tx ≤ t−ω}
                reverse_reachability.trim_before(lower_limit)
            # If the end result is ∅, the vertex key shall be deleted
            if len(reverse_reachability) == 0:
                del self._reverse_reachability[vertex]

    def _prune_reverse_reachabilities(self, current_time: Timestamp) -> None:
        lower_limit = current_time - self._omega
        for vertex in self._reverse_reachability.keys():
            self._prune_reverse_reachability(vertex=vertex, lower_limit=lower_limit)

    def _process_target_interaction(
            self, target_interaction: TargetInteraction
    ) -> dict[slice, Candidates]:
        """
        Updates reverse reachability and root interval tree
        when a new edge (u -> v) arrives at `current_timestamp`.

        :param target_interaction: (b, A, t)
            where 'A' is the set of source vertices, 'b' is the target_reachability vertex, and 't' is the timestamp.
        """
        # (a, b, t) ∈ E
        target = target_interaction.target
        sources = target_interaction.sources
        block_timestamp = target_interaction.timestamp
        target_reverse_reachability = self._reverse_reachability[target]

        # Output dict to indicate if new seeds are added
        new_seeds = {}
        for source in sources:
            if source not in self._reverse_reachability:
                continue
            # if S(a) exists then
            source_reverse_reachability = self._reverse_reachability[source]

            # for (b, tb) ∈ S(b) do
            cyclic_reachability = DirectReachability(
                v for v in target_reverse_reachability
                if v.vertex == target and v.timestamp < block_timestamp
            )
            # {c ∈ S(a), tc > tb}
            for cyclic_reachable in cyclic_reachability:
                # C ← {c | (c,tc) ∈ S(a),tc > tb} ∪ {a}
                candidate_reachability = DirectReachability(source_reverse_reachability)
                candidates = Candidates([source])
                candidates.update(c.vertex for c in candidate_reachability)
                if len(candidates) > 1:
                    # Output (b, [tb, t], C)
                    seed_range = slice(cyclic_reachable.timestamp, block_timestamp)
                    new_seeds[seed_range] = candidates

        # Remove to avoid duplicate output
        # S(b) ← S(b) \ {(b, tb)}
        self._reverse_reachability[target] = DirectReachability(
            reverse_reachable
            for reverse_reachable in target_reverse_reachability
            if reverse_reachable.vertex != target
        )
        return new_seeds

    def process_batch(self, batch: TransactionBlock) -> None:
        pruned_reachabilities = set()
        lower_limit = batch.timestamp - self._omega
        for target, sources in batch.items():
            # Add reachability entry
            # S(b) ← S(b) ∪ {(a, t)}
            for vertex in [target, *sources]:
                if vertex not in pruned_reachabilities:
                    self._prune_reverse_reachability(vertex=vertex, lower_limit=lower_limit)
            self._reverse_reachability[target].add(PointVertices(vertices=sources, timestamp=batch.timestamp))

        new_target_reachabilities = {}
        for target, sources in batch.items():
            target_reachability = self._reverse_reachability[target]
            source_reachabilities = [
                self._reverse_reachability[source]
                for source in sources
                if source in self._reverse_reachability
            ]
            new_target_reachabilities[target] = DirectReachability.union(target_reachability, *source_reachabilities)

        # Update the target_reachability reachabilities
        for target, target_reachability in new_target_reachabilities.items():
            self._reverse_reachability[target] = target_reachability

        for target, sources in batch.items():
            target_interaction = TargetInteraction(target=target, sources=sources, timestamp=batch.timestamp)
            new_seeds = self._process_target_interaction(target_interaction=target_interaction)
            for seed_range, candidates in new_seeds.items():
                root_interval_tree = self._root_interval_trees[target]
                # Add new seeds
                root_interval_tree[seed_range] = candidates
                # Merges adjacent or overlapping intervals for root `v` within the allowed timestamp window.
                if len(root_interval_tree) > 1:
                    root_interval_tree.merge_enclosed(self._omega)

    def pop_primed_seeds(self, upper_limit: Timestamp | None = None) -> list[Seed]:
        """
        Retrieve, pop and return primed seeds from the root interval trees.

        :param upper_limit: latest timestamp to consider for primed seeds.
        :return:
        """
        # Initialize a list to store primed seeds
        primed_seeds: list[Seed] = []
        # Iterate over each root and its associated interval tree
        to_delete = []
        for root in self._root_interval_trees.keys():
            interval_tree = self._root_interval_trees[root]
            # with self._root_interval_trees.access(root) as interval_tree:
            # Check if the interval tree contains intervals starting before or at the cutoff time
            primed_intervals = []
            all_intervals = False
            # There is no upper limit, collect all intervals of all interval trees
            # or
            # Interval tree lies before upper limit, collect whole interval tree
            if upper_limit is None or interval_tree.end() <= upper_limit:
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
            # If the interval tree is empty after processing, mark the root for deletion
            if len(interval_tree) == 0:
                to_delete.append(root)
        # Remove the root from the tree dictionary if its interval tree is now empty
        for root in to_delete:
            del self._root_interval_trees[root]
        # Return the list of gathered seeds
        return primed_seeds

    # def wait(self) -> None:
    #     """
    #     Wait for all running tasks to complete.
    #     """
    #     wait(self._running_tasks)

    def cleanup(self, current_time: Timestamp):
        self._prune_reverse_reachabilities(current_time=current_time)


class SeedExplorer:
    def __init__(self, omega: Timedelta):
        # Maximum timestamp window for relevant edges
        self._omega: Timedelta = omega
        # Thread pool for concurrent execution
        self._thread_pool: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=os.cpu_count() // 2)
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
        for seed, future in self._running_tasks.items():
            if future.done():
                cycle_graphs = future.result()
                detected_graphs[seed] = cycle_graphs
        # Remove completed tasks from the running tasks
        for seed in detected_graphs.keys():
            self._explored_seeds.add(seed)
            del self._running_tasks[seed]

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
