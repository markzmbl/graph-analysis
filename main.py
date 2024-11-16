import argparse
import subprocess
from pathlib import Path
from utils import GraphEdgeIterator, parse_arguments, to_cli_arguments


def main(*args, **kwargs):
    # Parse command-line arguments
    args = parse_arguments(**kwargs)
    start_date = args.start_date
    end_date = args.end_date
    del args.start_date, args.end_date
    input_path = Path("/Users/mark/Documents/uni/master/graph-analysis/CycleDetection/dataset/edge_list.csv")
    output_path = Path("test.csv")

    # Initialize GraphEdgeIterator
    graph_iterator = GraphEdgeIterator()

    # Path to the compiled C program
    program_path = Path("./CycleDetection/cmake-build-debug/CycleDetection")
    command = [program_path, f"-i {input_path}", f"-o {output_path}", *to_cli_arguments(args)]

    # Run the program and capture the output in real-time
    process = subprocess.Popen(
        command,  # Command to run
        stdout=subprocess.PIPE,  # Capture standard output
        stderr=subprocess.PIPE,  # Capture standard error
        text=True,  # Decode bytes to strings
        bufsize=1  # Line buffering
    )


    try:
        # Monitor the output in real-time
        print("Monitoring program output:")
        with process.stdout as stdout, process.stderr as stderr:
            for line in iter(stdout.readline, ""):
                print("STDOUT:", line, end="")  # Real-time output from stdout

            for line in iter(stderr.readline, ""):
                print("STDERR:", line, end="")  # Real-time output from stderr

        # Wait for the process to complete
        process.wait()

        # Optional: Get exit status
        exit_code = process.returncode
        print(f"\nProgram exited with code {exit_code}")

    except KeyboardInterrupt:
        # Handle manual interruption gracefully
        print("\nTerminating program...")
        process.terminate()
        process.wait()

if __name__ == "__main__":
    main("test.csv")
