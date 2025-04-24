import csv
import json
import os
import argparse
from datetime import date
from decimal import Decimal
from multiprocessing import freeze_support
from pathlib import Path

from networkx.readwrite import json_graph

from input.iterator import GraphEdgeIterator
from dscent.iterator import GraphCycleIterator
import networkx as nx
from networkx.readwrite.json_graph import node_link_data
from tqdm import tqdm
import pickle

_DEFAULT_START_DATE = "2021-10-01"
_DEFAULT_END_DATE = "2022-07-01"
# _DEFAULT_END_DATE = "2021-10-07"
_DEFAULT_OMEGA = 25
_DEFAULT_WORKERS = 2
# _DEFAULT_WORKERS = 0
_DEFAULT_GC_MAX = "48GB"
# _DEFAULT_GC_MAX = "100MB"
_DEFAULT_GC_COOLDOWN = 10_000_000
# _DEFAULT_GC_COOLDOWN = 10_000
_DEFAULT_LOG_PREFIX = "log"
_DEFAULT_LOG_DIR = "."
_DEFAULT_PROGRESS_BAR = True


# Function to convert non-serializable types
def serialize_value(value):
    if isinstance(value, bytes):
        return value.hex()  # Or use base64.b64encode(value).decode('utf-8') for base64
    elif isinstance(value, Decimal):
        return float(value)
    return value


def serialize_graph_data(graph):
    data = node_link_data(graph, edges="edges")
    # Fix edge data
    for edge in data["edges"]:
        for key in edge:
            edge[key] = serialize_value(edge[key])
    # Fix node data (if needed)
    for node in data["nodes"]:
        for key in node:
            node[key] = serialize_value(node[key])
    return data


def main(
        start_date: str | date = _DEFAULT_START_DATE,
        end_date: str | date = _DEFAULT_END_DATE,
        omega: int = _DEFAULT_OMEGA,
        workers: int = _DEFAULT_WORKERS,
        gc_max: str | int = _DEFAULT_GC_MAX,
        gc_cooldown: int = _DEFAULT_GC_COOLDOWN,
        log_prefix: str = _DEFAULT_LOG_PREFIX,
        log_directory: str | Path = _DEFAULT_LOG_DIR,
        progress_bar: bool = _DEFAULT_PROGRESS_BAR,
):
    # meebits = "7bd29408f11d2bfc23c34f18275bbf23bb716bc7"
    # g = nx.MultiDiGraph()

    log_directory = Path(log_directory)
    memory_log_path = log_directory / f"{log_prefix}_memory.csv"
    cycles_log_path = log_directory / f"{log_prefix}_cycles.csv"
    with(
        memory_log_path.open("w") as memory_log_stream,
        cycles_log_path.open("w") as cycles_log_stream,
    ):
        cycles_csv_writer = csv.writer(cycles_log_stream, delimiter=';')
        interactions = GraphEdgeIterator(start_date=start_date, end_date=end_date)
        cycles_csv_writer.writerow(["seed_begin", "seed_end", "next_seed_begin", "candidates", "bundled_cycle"])
        for seed, bundled_cycle_graph in GraphCycleIterator(
                interactions,
                omega=omega, max_workers=workers,
                logging_interval=1,
                garbage_collection_max=gc_max,
                garbage_collection_cooldown=gc_cooldown,
                log_stream=memory_log_stream,
                yield_seeds=True,
                progress_bar=progress_bar,
        ):
            cycles_csv_writer.writerow([
                seed.interval.begin,
                seed.interval.end,
                seed.next_begin,
                json.dumps(list(seed.candidates)),
                json.dumps(serialize_graph_data(bundled_cycle_graph)),
            ])
            cycles_log_stream.flush()

    # with open("meebits.pickle", "wb") as f:
    #     pickle.dump(g, f)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process graph interactions and detect cycles.")
    parser.add_argument("--start_date", default=_DEFAULT_START_DATE, type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end_date", default=_DEFAULT_END_DATE, type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("--omega", default=_DEFAULT_OMEGA, type=int, help="Omega value for cycle detection")
    parser.add_argument("--workers", default=_DEFAULT_WORKERS, type=int, help="Number of worker threads")
    parser.add_argument("--gc_max", default=_DEFAULT_GC_MAX, type=str, help="Garbage Collection maximum memory")
    parser.add_argument("--gc_cooldown", default=_DEFAULT_GC_COOLDOWN, type=int, help="Garbage Collection cooldown")
    parser.add_argument("--log_prefix", default=_DEFAULT_LOG_PREFIX, type=str, help="Log file prefix")
    parser.add_argument("--log_dir", default=_DEFAULT_LOG_DIR, type=str, help="Log file prefix")
    parser.add_argument("--progress", default=_DEFAULT_PROGRESS_BAR, type=bool, help="Show Progress Bar")
    args = parser.parse_args()
    main(
        start_date=args.start_date,
        end_date=args.end_date,
        omega=args.omega,
        workers=args.workers,
        gc_max=args.gc_max,
        log_prefix=args.log_prefix,
        progress_bar=args.progress,
    )
