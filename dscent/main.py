import argparse
import multiprocessing
from os import cpu_count

from matplotlib import pyplot as plt
import pandas as pd
from tqdm import tqdm

from input.iterator import GraphEdgeIterator
from cycles.iterator import GraphCycleIterator


def process_graph(thread_id, omega, start_date, end_date, max_workers, cleanup_interval, log_file, logging_interval):
    interaction_iterator = tqdm(GraphEdgeIterator(start_date, end_date), desc=f"ω = {omega:3}", position=thread_id)
    with open(log_file, "w") as log_stream:
        try:
            list(GraphCycleIterator(
                interaction_iterator,
                omega=omega,
                max_workers=max_workers,
                cleanup_interval=cleanup_interval,
                log_stream=log_stream,
                logging_interval=logging_interval
            ))
        except KeyboardInterrupt:
            pass


def main():
    parser = argparse.ArgumentParser(description="Process graph interactions and cycles.")
    parser.add_argument("--start-date", type=str, default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2021-01-01", help="End date (YYYY-MM-DD)")
    parser.add_argument("--omega-values", type=int, nargs="+", default=[10, 25, 50, 100], help="Four omega values to use")
    parser.add_argument("--max-workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--cleanup-interval", type=int, default=10_000, help="Cleanup interval")
    parser.add_argument("--log-file-prefix", type=str, default="log", help="Prefix for log files")
    parser.add_argument("--logging-interval", type=float, default=60, help="Interval on how frequently to measure logging values")

    args = parser.parse_args()
    processes = []
    for thread_id, omega in enumerate(args.omega_values):
        log_file = f"{args.log_file_prefix}_{omega}.csv"
        p = multiprocessing.Process(target=process_graph, args=(
            thread_id, omega, args.start_date, args.end_date, args.max_workers, args.cleanup_interval, log_file, args.logging_interval))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
