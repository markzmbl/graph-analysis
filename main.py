import argparse
from pathlib import Path

from utils import GraphEdgeIterator


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Monitor and fill a graph iterator if edges drop below a threshold.")
    parser.add_argument("row_limit", type=int, help="Minimum number of edges to maintain.")
    parser.add_argument("--pipe", type=str, default="/tmp/edge_pipe", help="Path to the named pipe.")
    args = parser.parse_args()

    # Initialize GraphEdgeIterator
    graph_iterator = GraphEdgeIterator()


if __name__ == "__main__":
    main()