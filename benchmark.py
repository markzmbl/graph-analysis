import argparse
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from meebits_analysis import load_transactions, OMEGAS

_DEFAULT_LOG_INTERVAL = 1  # Seconds
_DEFAULT_LOG_PREFIX = "2scent"
_DEFAULT_LOG_DIR = "2scent/log"

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
        interval: float = _DEFAULT_LOG_INTERVAL
):
    with open(output_csv, "w") as f:
        f.write("time_seconds,cycles_total,memory_usage_bytes\n")
        while not stop_event.is_set():
            _log_memory_usage_once(pid, f, monitor_csv)
            time.sleep(interval)
        # One final check after stop_event is set
        _log_memory_usage_once(pid, f, monitor_csv)


def main():
    parser = argparse.ArgumentParser(description="Process sub_graph interactions and cycles with 2SCENT.")
    parser.add_argument("--temporary-directory", type=str, default=None, help="Prefix for log files")
    parser.add_argument("--log-interval", default=_DEFAULT_LOG_INTERVAL, type=int, help="Logging interval")
    parser.add_argument("--log-prefix", default=_DEFAULT_LOG_PREFIX, type=str, help="Log file prefix")
    parser.add_argument("--log-dir", default=_DEFAULT_LOG_DIR, type=str, help="Log file directory")

    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_prefix = args.log_prefix

    transaction_df = load_transactions()
    actors = pd.unique(transaction_df[['from', 'to']].values.ravel())
    actor_mapping = {actor: idx for idx, actor in enumerate(actors)}

    graph_path = log_dir / "graph.csv"

    with open(graph_path, "w") as temp_file:
        for _, row in transaction_df.iterrows():
            u = actor_mapping[row["from"]]
            v = actor_mapping[row["to"]]
            t = int(row["timestamp"])
            temp_file.write(f"{u},{v},{t}\n")

    pbar = tqdm(OMEGAS)
    for omega in pbar:
        pbar.set_description(f"{omega}")
        window = str(int(pd.to_timedelta(omega).total_seconds()))
        log_file = log_dir / f"{log_prefix}_{omega}.log"
        output_csv = log_dir / f"{log_prefix}_{omega}.csv"
        mem_csv = log_dir / f"{log_prefix}_{omega}_memory.csv"

        with log_file.open("w") as log_output_file:
            process = subprocess.Popen(
                [
                    "2scent/CycleDetection/cmake-build-release/CycleDetection",
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
            monitor_csv = log_dir / f"graph-monitor-{window}-bundle.csv"
            mem_thread = threading.Thread(
                target=monitor_memory_usage,
                args=(process.pid, mem_csv, monitor_csv, stop_event)
            )
            mem_thread.start()

            process.wait()
            stop_event.set()
            mem_thread.join()


if __name__ == "__main__":
    main()
