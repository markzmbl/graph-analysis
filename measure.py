import json
from collections import defaultdict
from collections.abc import Callable

import networkx as nx
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm
from pandas.errors import ParserError

from dscent.graph import TransactionGraph
from dscent.time_sequence import get_sequence_after, get_sequence_before

from dscent.time_sequence import get_sequence_before, get_sequence_after
from collections import defaultdict

from collections import defaultdict
from datetime import timedelta, datetime

from serialize import serialize_graph, decode_graph, _CACHE_DIRECTORY


class AnalysisDataFrame(pd.DataFrame):
    _omega: np.timedelta64
    _name: str

    def __init__(self, *args, omega: np.timedelta64, name: str, graph_col: str = "graph", **kwargs):
        super().__init__(*args, **kwargs)
        self._omega = omega
        self._name = name

        if graph_col not in self.columns:
            raise ValueError(f"Must have graph column: '{graph_col}'")
        self._graph_col = graph_col
        self._decode_columns()

        self["vertices"] = self["graph"].apply(lambda g: list(g.nodes()))
        self["number_of_nodes"] = self["graph"].apply(lambda g: g.number_of_nodes())
        self["number_of_edges"] = self["graph"].apply(lambda g: g.number_of_edges())

        self["dwell"] = self["graph"].apply(get_average_minimum_dwell_time)

        for index, cycle in self["graph"].items():
            first, *_, last = sorted(cycle.edges(data=True), key=lambda x: x[2]["timestamp"])
            begin, end = first[2]["timestamp"], last[2]["timestamp"]
            begin, end = datetime.fromtimestamp(begin), datetime.fromtimestamp(end)
            self.loc[index, "begin"] = begin
            self.loc[index, "end"] = end

        self[["begin", "end"]] = self[["begin", "end"]].apply(pd.to_datetime)
        self["duration"] = self["end"] - self["begin"]

        self["price_delta"] = self["graph"].apply(get_average_price_delta)
        self["rate"] = (
                self["number_of_edges"]
                / self["number_of_nodes"]
                / self["duration"].apply(lambda g: g.total_seconds())
        )

        self["duration_minutes"] = self["duration"].dt.total_seconds() / 60
        self["duration_hours"] = self["duration"].dt.total_seconds() / 3600
        self["dwell_minutes"] = self["dwell"] / 60
        self["dwell_hours"] = self["dwell"] / 3600

    @property
    def omega(self) -> np.timedelta64:
        return self._omega

    @property
    def name(self) -> str:
        return self._name

    def _decode_columns(self):
        for col, dtype in self.dtypes.items():
            if dtype == object:
                decoded_series = None
                if col == self._graph_col:
                    decoded_series = self[self._graph_col].apply(decode_graph)
                elif pd.api.types.is_string_dtype(self[col]):
                    try:
                        decoded_series = self[col].apply(json.loads)
                    except (json.JSONDecodeError, TypeError):
                        try:
                            decoded_series = pd.to_datetime(self[col])
                        except (ValueError, TypeError, ParserError):
                            decoded_series = pd.to_timedelta(self[col])
                if decoded_series is not None:
                    self[col] = decoded_series

    def to_serializable(self) -> pd.DataFrame:
        serializable_df = self.copy()
        for col, dtype in self.dtypes.items():
            if dtype == object:
                serializable_df[col] = (
                    self[self._graph_col].apply(serialize_graph)
                    if col == self._graph_col
                    else self[col].apply(json.dumps)
                )
        return serializable_df.drop(columns=[
            "vertices",
            "number_of_nodes",
            "number_of_edges",
            "begin",
            "end",
            "duration",
            "duration_minutes",
            "duration_hours",
            "dwell",
            "dwell_minutes",
            "dwell_hours",
            "price_delta",
            "rate",
        ])


class DwellTimeMap:
    def __init__(self):
        self._data: defaultdict[str, defaultdict[str, list[timedelta]]] = defaultdict(lambda: defaultdict(list))

    def add(self, intermediate: str, sender: str, dwell_time: timedelta):
        self._data[intermediate][sender].append(dwell_time)

    def get(self, intermediate: str, sender: str) -> list[timedelta]:
        return self._data.get(intermediate, {}).get(sender, [])

    def intermediates(self) -> list[str]:
        return list(self._data.keys())

    def senders_for(self, intermediate: str) -> list[str]:
        return list(self._data.get(intermediate, {}).keys())

    def all(self) -> dict[str, dict[str, list[timedelta]]]:
        return self._data

    def aggregate(
            self,
            aggregator: Callable[[list[timedelta]], timedelta] | None = None
    ) -> dict[tuple[str, str], timedelta]:
        """
        Aggregate dwell times using a provided aggregator.

        Returns a flat dictionary: {(intermediate, sender): aggregated_value}

        If no aggregator is given, returns the flattened list:
            {(intermediate, sender): list of durations}
        """
        result = {}
        for intermediate, senders in self._data.items():
            for sender, durations in senders.items():
                if durations:
                    key = (intermediate, sender)
                    if aggregator:
                        result[key] = aggregator(durations)
                    else:
                        result[key] = durations
        return result

    def __repr__(self):
        return f"DwellTimeMap({dict(self._data)})"


def get_dwell_times(graph: TransactionGraph) -> DwellTimeMap:
    # Initialize custom data structure to store dwell times
    dwell_times = DwellTimeMap()

    # Iterate over all nodes in the graph, treating each as a potential sender
    for sender in graph.nodes():
        # Look at all direct connections from this sender
        for intermediate, incoming_transactions in graph[sender].items():
            # Skip self-loops
            if sender == intermediate:
                continue

            # Sort the timestamps of incoming transactions to the intermediate node
            incoming_transactions = sorted(incoming_transactions.timestamps())

            # Pair each incoming transaction with the next one to form time windows
            incoming_transaction_pairs = zip(incoming_transactions, incoming_transactions[1:] + [None])

            for incoming_timestamp, next_incoming_time in incoming_transaction_pairs:
                # Examine all outgoing transactions from the intermediate node
                for recipient, outgoing_transactions in graph[intermediate].items():
                    # Skip self-loops again
                    if intermediate == recipient:
                        continue

                    # Sort the timestamps of outgoing transactions from intermediate to recipient
                    outgoing_timestamps = sorted(outgoing_transactions.timestamps())

                    # Remove outgoing timestamps that are before the incoming timestamp (strictly)
                    outgoing_timestamps = get_sequence_after(
                        outgoing_timestamps, limit=incoming_timestamp, include_limit=False
                    )

                    # If there is a subsequent incoming transaction, trim away timestamps after it (inclusive)
                    if next_incoming_time is not None:
                        outgoing_timestamps = get_sequence_before(
                            outgoing_timestamps, limit=next_incoming_time, include_limit=True
                        )

                    # For valid outgoing timestamps, calculate dwell time and add to result set
                    for outgoing_timestamp in outgoing_timestamps:
                        dwell_time = outgoing_timestamp - incoming_timestamp
                        dwell_times.add(intermediate, sender, dwell_time)

    return dwell_times


def get_average_price_delta(graph: TransactionGraph) -> float:
    item_prices = defaultdict(list)
    transactions = sorted(
        (key.timestamp, data["token_id"], data["price_usd"])
        for *_, key, data in graph.edges(keys=True, data=True)
    )
    for _, token_id, price_usd in transactions:
        item_prices[token_id].append(price_usd)
    item_deltas = list(prices[-1] - prices[0] for prices in item_prices.values() if len(prices) > 1)
    if not item_deltas:
        return np.nan
    return float(sum(item_deltas) / len(item_prices))


def get_average_minimum_dwell_time(graph: TransactionGraph) -> float:
    """
    Computes the average minimum dwell time in the transaction graph.
    """
    dwell_times = get_dwell_times(graph=graph)
    min_dwell_times = list(dwell_times.aggregate(aggregator=min).values())
    return np.mean(min_dwell_times) if min_dwell_times else np.nan


def get_burstiness(
        graph: TransactionGraph,
        aggregator: Callable[[list[timedelta]], timedelta] = min
) -> float:
    """
    Computes the burstiness of dwell times using:
        (σ - μ) / (σ + μ)

    Burstiness ∈ (-1, 1), where:
        -1 = completely regular
         0 = Poisson-like
         1 = highly bursty
    """
    dwell_times = get_dwell_times(graph)

    durations = list(dwell_times.aggregate(aggregator=aggregator).values())

    if not durations:
        return np.nan

    sigma = np.std(durations)
    mu = np.mean(durations)

    if sigma == mu:
        return 0.0

    return float((sigma - mu) / (sigma + mu))


def compute_component_df(cycle_df: AnalysisDataFrame):
    edge_list = []
    n = len(cycle_df)
    pbar = tqdm(total=(n * (n - 1)) // 2, desc="Building meta graph", unit_scale=True)

    for i in range(n):
        begin_i = cycle_df.at[i, "begin"]
        end_i = cycle_df.at[i, "end"]
        vertices_i = set(cycle_df.at[i, "vertices"])

        for j in range(i + 1, n):
            begin_j = cycle_df.at[j, "begin"]
            end_j = cycle_df.at[j, "end"]
            vertices_j = set(cycle_df.at[j, "vertices"])

            # Check if intervals [begin_i, end_i] and [begin_j, end_j] overlap
            if (end_i + cycle_df.omega >= begin_j) and (end_j >= begin_i - cycle_df.omega):
                # Check if they share any vertices
                if not vertices_i.isdisjoint(vertices_j):
                    edge_list.append((i, j))
            pbar.update(1)

    # Build and analyze the meta graph
    meta_graph = nx.Graph()
    meta_graph.add_edges_from(edge_list)

    components = list(nx.connected_components(meta_graph))
    print(f"Found {len(components)} connected components in the meta graph.")

    # Assign 'component', 'min_begin', and 'max_end' attributes
    for i, component in enumerate(nx.connected_components(meta_graph)):
        for cycle_index in component:
            cycle_df.loc[cycle_index, "component"] = i

    # Group by 'component' and compute the desired statistics
    component_df = cycle_df.reset_index().groupby("component").agg(
        begin=("begin", "min"),
        end=("end", "max"),
        avg_cycle_duration=("duration", "mean"),
        cycles=("index", list),
        vertices=("vertices", lambda x: list(set().union(*x))),
        graph=("graph", lambda x: TransactionGraph.from_networkx(nx.compose_all(x)))
    ).reset_index()
    return component_df


def draw_graph(graph):
    # Define the layout
    pos = nx.spring_layout(graph)

    # Draw nodes and labels
    nx.draw_networkx_nodes(graph, pos, node_size=500)
    nx.draw_networkx_labels(graph, pos)

    # Draw each edge with a different curvature
    edges = list(graph.edges(keys=True))
    for i, (u, v, k) in enumerate(edges):
        nx.draw_networkx_edges(
            graph,
            pos,
            edgelist=[(u, v)],
            connectionstyle=f'arc3,rad={(i - len(edges) / 2) * 0.2}',
            edge_color='black'
        )

    plt.axis('off')
    plt.show()


def load_cycles(omega: np.timedelta64) -> AnalysisDataFrame:
    cycles_path = _CACHE_DIRECTORY / f"{omega}_cycles.csv"
    cycle_df = AnalysisDataFrame(
        pd.read_csv(cycles_path, delimiter=";"), omega=omega, name="Cycles"
    )
    return cycle_df


def get_component_df(cycle_df, omega=None):
    cache_path = _CACHE_DIRECTORY / f"{cycle_df.omega}_components.csv"
    if cache_path.exists():
        component_df = AnalysisDataFrame(
            pd.read_csv(cache_path, delimiter=";"), omega=cycle_df.omega, name="Components"
        )
    else:
        component_df = AnalysisDataFrame(compute_component_df(cycle_df), omega=cycle_df.omega, name="Components")
        component_df.to_serializable().to_csv(cache_path, sep=";", index=False)
    return component_df


if __name__ == '__main__':
    class tmp:
        pass


    tmp = tmp()
    tmp.omega = np.timedelta64(10, "m")
    print(get_component_df(tmp).to_serializable()["graph"])
