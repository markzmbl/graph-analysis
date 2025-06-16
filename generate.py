import argparse
import time
from datetime import date
from pathlib import Path

from input.iterator import GraphEdgeIterator
from dscent.iterator import GraphCycleIterator

# _DEFAULT_START_DATE = "2021-10-01"
_DEFAULT_START_DATE = None
# -----
# _DEFAULT_END_DATE = "2021-10-10"
_DEFAULT_END_DATE = None
# -----
_BUFFERED_FILES_COUNT = 2
_DEFAULT_OMEGA = 25
# -----
_DEFAULT_GC_MAX = "32GB"
# -----
_DEFAULT_LOG_INTERVAL = 60
_DEFAULT_LOG_PREFIX = "log"
_DEFAULT_LOG_DIR = "."
_DEFAULT_PROGRESS_BAR = True
_FLUSH_INTERVAL_SECONDS = 60 * 60  # 1 hour


def main(
        buffered_files_count: int = _BUFFERED_FILES_COUNT,
        start_date: str | date = _DEFAULT_START_DATE,
        end_date: str | date = _DEFAULT_END_DATE,
        omega: int = _DEFAULT_OMEGA,
        gc_max: str | int = _DEFAULT_GC_MAX,
        log_interval: int = _DEFAULT_LOG_INTERVAL,
        log_prefix: str = _DEFAULT_LOG_PREFIX,
        log_directory: str | Path = _DEFAULT_LOG_DIR,
        progress_bar: bool = _DEFAULT_PROGRESS_BAR,
):
    log_directory = Path(log_directory)
    memory_log_path = log_directory / f"{log_prefix}_memory.csv"
    with memory_log_path.open("w") as memory_log_stream:
        interactions = GraphEdgeIterator(start_date=start_date, end_date=end_date, buffer_count=buffered_files_count)
        total = len(interactions)
        interactions = iter(
            (u, v, blocknum, {"timestamp": blocknum})
            for u, v, blocknum, _
            in interactions
        )
        cycle_iterator = GraphCycleIterator(
            interactions=interactions,
            total=total,
            omega=omega,
            garbage_collection_max=gc_max,
            logging_interval=log_interval,
            log_stream=memory_log_stream,
            progress_bar=progress_bar,
        )
        last_flush_time = time.monotonic()
        for item in cycle_iterator:
            # Handle item if needed
            now = time.monotonic()
            if now - last_flush_time >= _FLUSH_INTERVAL_SECONDS:
                memory_log_stream.flush()
                last_flush_time = now


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process graph interactions and detect cycles.")
    parser.add_argument("--buffer", default=_BUFFERED_FILES_COUNT, type=int,
                        help="Define how many pickle files to keep loaded")
    parser.add_argument("--omega", default=_DEFAULT_OMEGA, type=int, help="Omega value for cycle detection")
    parser.add_argument("--gc_max", default=_DEFAULT_GC_MAX, type=str, help="Garbage Collection maximum memory")
    parser.add_argument("--log_interval", default=_DEFAULT_LOG_INTERVAL, type=int, help="Logging interval")
    parser.add_argument("--log_prefix", default=_DEFAULT_LOG_PREFIX, type=str, help="Log file prefix")
    parser.add_argument("--log_dir", default=_DEFAULT_LOG_DIR, type=str, help="Log file directory")
    parser.add_argument("--progress", default=_DEFAULT_PROGRESS_BAR, type=bool, help="Show Progress Bar")
    args = parser.parse_args()
    main(
        buffered_files_count=args.buffer,
        omega=args.omega,
        gc_max=args.gc_max,
        log_interval=args.log_interval,
        log_directory=args.log_dir,
        log_prefix=args.log_prefix,
        progress_bar=args.progress,
    )
