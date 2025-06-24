import csv
import json
from pathlib import Path

import numpy as np
import networkx as nx
import pandas as pd
from networkx.readwrite.json_graph.node_link import node_link_data

from dscent.graph import TransactionGraph
from dscent.iterator import GraphCycleIterator
from serialize import serialize_graph

OMEGAS = (
    np.timedelta64(3, 'm'),
    np.timedelta64(4, 'm'),
    np.timedelta64(5, 'm'),
    np.timedelta64(6, 'm'),
    np.timedelta64(7, 'm'),
    np.timedelta64(8, 'm'),
    np.timedelta64(9, 'm'),
    np.timedelta64(10, 'm'),
    np.timedelta64(15, 'm'),
    np.timedelta64(30, 'm'),
    np.timedelta64(1, 'h'),
    np.timedelta64(2, 'h'),
    np.timedelta64(6, 'h'),
)


def load_transactions():
    df = pd.read_csv("meebits.csv")
    df.index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df.sort_index(inplace=True)
    mask = (
            ~pd.isna(df["buyer"])  # only with buyer
            & (df["to"] == df["buyer"])  # only transfers to buyer
            & ~pd.isna(df['price_usd'])  # only with price in USD
    )
    df = df[mask].drop(columns="buyer")
    return df


def create_transaction_graph(transaction_df: pd.DataFrame | None = None) -> TransactionGraph:
    if transaction_df is None:
        transaction_df = load_transactions()
    transaction_graph = TransactionGraph.from_networkx(
        nx.from_pandas_edgelist(
            transaction_df,
            source='from', target='to', edge_attr=True,
            create_using=nx.MultiDiGraph
        )
    )
    return transaction_graph


def main():
    transaction_graph = create_transaction_graph()
    transactions = sorted(transaction_graph.edges(keys=True, data=True), key=lambda x: x[3]['timestamp'])

    gc_max = "32GB"
    log_directory = Path("cache")

    log_interval = 1  # seconds

    for omega in OMEGAS:
        log_prefix = f"{omega}"

        memory_log_path = log_directory / f"{log_prefix}_memory.csv"
        cycles_log_path = log_directory / f"{log_prefix}_cycles.csv"
        with(
            memory_log_path.open("w") as memory_log_stream,
            cycles_log_path.open("w") as cycles_log_stream,
        ):
            cycles_csv_writer = csv.writer(cycles_log_stream, delimiter=';')
            cycles_csv_writer.writerow(["seed_begin", "seed_end", "next_seed_begin", "candidates", "graph"])
            for seed, bundled_cycle_graph in GraphCycleIterator(
                    transactions,
                    omega=omega,
                    garbage_collection_max=gc_max,
                    logging_interval=log_interval,
                    log_stream=memory_log_stream,
                    yield_seeds=True,
                    progress_bar=True,
            ):
                cycles_csv_writer.writerow([
                    seed.interval.begin,
                    seed.interval.end,
                    seed.next_begin,
                    json.dumps(list(seed.candidates)),
                    serialize_graph(TransactionGraph.from_networkx(bundled_cycle_graph)),
                ])
                cycles_log_stream.flush()


if __name__ == '__main__':
    main()
