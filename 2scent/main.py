import argparse
import subprocess
import tempfile

from input.iterator import GraphEdgeIterator


def main():
    parser = argparse.ArgumentParser(description="Process sub_graph interactions and cycles with 2SCENT.")
    parser.add_argument("--start-date", type=str, default="2020-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2020-01-02", help="End date (YYYY-MM-DD)")
    parser.add_argument("--omega", type=int, default=10, help="Omega value to use")
    parser.add_argument("--cycle-length", type=int, default=80, help="Maximum Cycle length to use")
    parser.add_argument("--max-workers", type=int, default=2, help="Number of worker threads")
    parser.add_argument("--cleanup-interval", type=int, default=100_000, help="Cleanup interval")
    parser.add_argument("--file-prefix", type=str, default="2scent", help="Prefix for files")
    parser.add_argument("--temporary-directory", type=str, default=None, help="Prefix for log files")

    args = parser.parse_args()

    """Writes a fresh CSV file whenever requested"""
    with tempfile.TemporaryDirectory(dir=tempfile.tempdir) as temp_dir:
        temp_path = f"{temp_dir}/sub_graph.csv"

        with open(temp_path, "w") as temp_file:
            for u, v, t in GraphEdgeIterator(args.start_date, args.end_date):
                temp_file.write(f"{u},{v},{t}\n")

        with open(f"{args.file_prefix}.log", "w") as log_file:
            # Run the compiled C program
            subprocess.run([
                "CycleDetection/cmake-build-release/CycleDetection",
                "-i", temp_path,
                "-o", f"{args.file_prefix}.csv",
                "-w", str(args.omega),
                "-l", str(args.cycle_length),
                "-p", str(args.cleanup_interval), "-b", "1",
            ], stdout=log_file, stderr=subprocess.STDOUT, text=True)


if __name__ == "__main__":
    main()
