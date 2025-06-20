import argparse
import csv
import os
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from dscent.iterator import GraphCycleIterator
from meebits_analysis import load_transactions, OMEGAS, create_transaction_graph

_LOG_INTERVAL = 1  # Seconds
_2SCENT_DIR = "2scent/cache"
_LOG_DIR = "benchmark"

import time
import subprocess
import threading
from pathlib import Path


def _log_memory_usage_once(pid: int, f, monitor_csv: Path):
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True, text=True, check=True
        )
        rss_kb = int(result.stdout.strip())
        rss_bytes = rss_kb * 1024

        try:
            with open(monitor_csv, "r") as outf:
                line_count = sum(1 for _ in outf)
                if line_count > 0:
                    line_count -= 1
        except FileNotFoundError:
            line_count = 0

        f.write(f"{time.monotonic()},{line_count},{rss_bytes}\n")
        f.flush()

    except (subprocess.CalledProcessError, ValueError):
        pass  # Safe fail; don't raise, especially during the final call


def monitor_memory_usage(
        pid: int, output_csv: Path, monitor_csv: Path, stop_event: threading.Event,
        interval: float = _LOG_INTERVAL
):
    with open(output_csv, "w") as f:
        f.write("time_seconds,cycles_total,memory_usage_bytes\n")
        while not stop_event.is_set():
            _log_memory_usage_once(pid, f, monitor_csv)
            time.sleep(interval)
        # One final check after stop_event is set
        _log_memory_usage_once(pid, f, monitor_csv)


def main():
    _2scent_dir = Path(_2SCENT_DIR)
    _2scent_dir.mkdir(parents=True, exist_ok=True)
    log_dir = Path(_LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)

    transaction_df = load_transactions()
    transaction_graph = create_transaction_graph()
    transactions = sorted(transaction_graph.edges(keys=True, data=True), key=lambda x: x[3]['timestamp'])

    actors = pd.unique(transaction_df[['from', 'to']].values.ravel())
    actor_mapping = {actor: idx for idx, actor in enumerate(actors)}

    graph_path = _2scent_dir / "graph.csv"

    with open(graph_path, "w") as temp_file:
        for _, row in transaction_df.iterrows():
            u = actor_mapping[row["from"]]
            v = actor_mapping[row["to"]]
            t = int(row["timestamp"])
            temp_file.write(f"{u},{v},{t}\n")

    run = 0
    while True:
        pbar = tqdm(OMEGAS)
        for omega in pbar:
            pbar.set_description(f"{omega}")
            window = str(int(pd.to_timedelta(omega).total_seconds()))
            log_file = _2scent_dir / f"{omega}_{run}.log"
            output_csv = _2scent_dir / f"{omega}_{run}.csv"
            mem_csv = log_dir / f"2scent_{omega}_{run}.csv"

            with log_file.open("w") as log_output_file:
                monitor_csv = _2scent_dir / f"graph-monitor-{window}-bundle.csv"
                monitor_csv.unlink(missing_ok=True)

                process = subprocess.Popen(
                    [
                        "2scent/ethereum2.csv/CycleDetection",
                        "-i", graph_path,
                        "-o", str(output_csv),
                        "-w", window,
                        "-b", "1",
                        "-z", "1"
                    ],
                    stdout=log_output_file,
                    stderr=subprocess.STDOUT,
                    text=True
                )

                stop_event = threading.Event()

                mem_thread = threading.Thread(
                    target=monitor_memory_usage,
                    args=(process.pid, mem_csv, monitor_csv, stop_event)
                )
                mem_thread.start()

                process.wait()
                stop_event.set()
                mem_thread.join()

                memory_log_path = log_dir / f"dscent_{omega}_{run}.csv"
                with memory_log_path.open("w") as memory_log_stream:
                    for _ in GraphCycleIterator(
                            transactions,
                            omega=omega,
                            logging_interval=_LOG_INTERVAL,
                            log_stream=memory_log_stream,
                            progress_bar=True,
                    ):
                        pass
            break
        run += 1
        break


if __name__ == "__main__":
    main()
