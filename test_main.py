import argparse
import multiprocessing
from os import cpu_count

from matplotlib import pyplot as plt
import pandas as pd
from tqdm import tqdm

from input.iterator import GraphEdgeIterator
from dscent.iterator import GraphCycleIterator


def main():
    parser = argparse.ArgumentParser(description="Process sub_graph interactions and cycles.")
    parser.add_argument("--start-date", type=str, default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2020-02-01", help="End date (YYYY-MM-DD)")
    parser.add_argument("--omega", type=int, default=10, help="Omega value to use")
    parser.add_argument("--max-workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--cleanup-interval", type=int, default=10_000, help="Cleanup interval")
    parser.add_argument("--log-file-prefix", type=str, default="log", help="Prefix for log files")
    parser.add_argument("--logging-interval", type=float, default=60, help="Interval on how frequently to measure logging values")

    args = parser.parse_args()
    log_file = f"{args.log_file_prefix}_{args.omega}.csv"
    interaction_iterator = tqdm(GraphEdgeIterator(args.start_date, args.end_date), desc=f"ω = {args.omega:3}")

    with open(log_file, "w") as log_stream:
        for cycle in GraphCycleIterator(
            interaction_iterator,
            omega=args.omega,
            max_workers=args.max_workers,
            cleanup_interval=args.cleanup_interval,
            log_stream=log_stream,
            logging_interval=args.logging_interval
        ):
            print(str(cycle))


if __name__ == "__main__":
    main()
