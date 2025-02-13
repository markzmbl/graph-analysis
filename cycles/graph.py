from __future__ import annotations


import networkx as nx

from cycles.types import TimeStamp, TimeDelta


class TransactionGraph(nx.MultiDiGraph):
    def begin(self) -> TimeStamp:
        *_, end_timestamp = next(iter(self.edges(keys=True)))
        return end_timestamp

    def length(self) -> TimeDelta:
        timestamps = [timestamp for *_, timestamp in self.edges(keys=True)]
        timestamps = sorted(timestamps)
        if not timestamps:
            return 0
        return timestamps[-1] - timestamps[0]

    def time_slice(self, begin: TimeStamp | None = None, end: TimeStamp | None = None):
        """
        Returns a subgraph view that filters edges based on the temporal constraints.
        Assumes edges have a timestamp as their key.

        :param begin: Minimum timestamp (inclusive)
        :param end: Maximum timestamp (exclusive)
        :return: A subgraph view with filtered edges
        """

        def edge_filter(u, v, key):
            return (begin is None or key >= begin) and (end is None or key <= end)

        return nx.subgraph_view(self, filter_edge=edge_filter)

    def prune(self, lower_time_limit: TimeStamp):
        self.remove_edges_from(list(self.time_slice(end=lower_time_limit).edges(keys=True)))
