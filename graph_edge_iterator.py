import argparse
import pickle
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, date
from pathlib import Path
import networkx as nx

DATE_FORMAT = "%Y-%m-%d"


def parse_date(date_str: str) -> date:
    """
    Parse a date string in the format YYYY-MM-DD and return a `datetime.date` object.

    Parameters
    ----------
    date_str : str
        The date string to parse, expected to be in YYYY-MM-DD format.

    Returns
    -------
    date
        A `datetime.date` object representing the given date string.

    Raises
    ------
    argparse.ArgumentTypeError
        If the input string does not match the YYYY-MM-DD format.

    Examples
    --------
    >>> parse_date("2025-01-23")
    datetime.date(2025, 1, 23)
    """
    try:
        return datetime.strptime(date_str, DATE_FORMAT).date()
    except ValueError:
        # Raise an argparse-specific error if format is invalid
        raise argparse.ArgumentTypeError(f"Invalid date format: '{date_str}'. Use YYYY-MM-DD.")


def read_graph(graph_path: Path) -> nx.MultiDiGraph:
    """
    Read a pickled NetworkX MultiDiGraph from the given file path.

    Parameters
    ----------
    graph_path : Path
        The file path pointing to a pickled MultiDiGraph object.

    Returns
    -------
    nx.MultiDiGraph
        The graph object unpickled from the specified file.

    Raises
    ------
    IOError
        If there is an issue opening or reading the file.

    Examples
    --------
    >>> from pathlib import Path
    >>> g = read_graph(Path("sample_graph.pickle"))
    >>> isinstance(g, nx.MultiDiGraph)
    True
    """
    with open(graph_path, "rb") as f:
        return pickle.load(f)


def parse_filename(graph_path: Path) -> tuple[int, date]:
    """
    Parse the filename to extract a block number and a date, assuming
    a naming convention like: 'blockNumber__YYYY-MM-DD__...'

    Parameters
    ----------
    graph_path : Path
        The file path whose stem (filename without extension) follows the pattern:
        blockNumber__YYYY-MM-DD__[__optional_parts]

    Returns
    -------
    tuple of (int, date)
        A tuple consisting of:
          - block_number (int): The parsed block number from the filename.
          - graph_date (date): The parsed date (YYYY-MM-DD) from the filename.

    Raises
    ------
    argparse.ArgumentTypeError
        If the date part in the filename cannot be parsed (invalid format).

    Examples
    --------
    >>> from pathlib import Path
    >>> parse_filename(Path("12__2024-12-31__someinfo.pickle"))
    (12, datetime.date(2024, 12, 31))
    """
    block_number, graph_date_str, *_ = graph_path.stem.split("__")
    graph_date = parse_date(graph_date_str)
    return int(block_number), graph_date


def get_graph_paths(
        start_date: date | str | None = None,
        end_date: date | str | None = None
) -> list[Path]:
    """
    Retrieve a sorted list of `.pickle` graph file paths from a fixed directory,
    optionally filtered by a start and end date. Filenames are expected to contain
    a date in the format YYYY-MM-DD (parsed via `parse_filename`).

    Parameters
    ----------
    start_date : date or str or None, optional
        The earliest date to include. Files with a date older than this will be excluded.
        If a string is provided, it should be in YYYY-MM-DD format. Defaults to None,
        meaning no lower bound.
    end_date : date or str or None, optional
        The latest date to include. Files with a date later than this will be excluded.
        If a string is provided, it should be in YYYY-MM-DD format. Defaults to None,
        meaning no upper bound.

    Returns
    -------
    list of Path
        A list of file paths pointing to `.pickle` files that match the optional date range.

    Notes
    -----
    - Files are assumed to be located in the directory "16TB/graphs_cleaned".
    - Each file is expected to be named in a way that `parse_filename` can
      extract its date (e.g., "12__2025-01-23__suffix.pickle").
    - The returned list is sorted by filename (lexicographically), which typically
      aligns with the chronological order if the naming conventions are consistent.

    Examples
    --------
    >>> # Get all graph paths from "16TB/graphs_cleaned" between 2025-01-01 and 2025-01-31:
    >>> paths = get_graph_paths(start_date="2025-01-01", end_date="2025-01-31")
    >>> for p in paths:
    ...     print(p)
    16TB/graphs_cleaned/1__2025-01-01__graph.pickle
    16TB/graphs_cleaned/2__2025-01-15__graph.pickle
    ...
    """
    # Convert start_date and end_date strings to date objects if necessary
    if start_date is not None and not isinstance(start_date, date):
        start_date = parse_date(start_date)
    if end_date is not None and not isinstance(end_date, date):
        end_date = parse_date(end_date)

    # Locate all .pickle files in the target directory
    graph_paths = sorted(Path("16TB/graphs_cleaned").glob("*.pickle"))

    # Filter based on the provided date range
    if start_date is not None or end_date is not None:
        filtered_paths = []
        for graph_path in graph_paths:
            _, graph_date = parse_filename(graph_path)
            # Keep the file if it's within [start_date, end_date]
            if (
                    (start_date is None or graph_date >= start_date)
                    and (end_date is None or graph_date <= end_date)
            ):
                filtered_paths.append(graph_path)
        graph_paths = filtered_paths

    return graph_paths


class GraphEdgeIterator:
    """
    An iterator that asynchronously reads multiple graph files (each file
    containing edges), sorts the edges of each file by their timestamp,
    and yields them in chronological order.

    Parameters
    ----------
    start_date : date or str or None, optional
        Earliest date for which graph files should be loaded.
        If None (default), starts from the earliest available graph file.
    end_date : date or str or None, optional
        Latest date for which graph files should be loaded.
        If None (default), continues until no more graph files are found.
    buffer_count : int, optional
        Number of graph "buffers" to maintain. Each buffer slot holds
        a `Future` that is reading a graph file in the background.
        Defaults to 2.

    Attributes
    ----------
    graph_path_iterator : Iterator[str]
        An iterator over all graph file paths (based on date range).
    buffer_count : int
        The maximum number of concurrent graph loading operations to keep.
    buffer : list of Future or None
        A circular-like buffer storing up to `buffer_count` Future objects.
        Each Future, when resolved, contains a loaded graph.
        A `None` entry indicates no further graphs are available.
    current_edges : Iterator[Tuple[int, int, int]]
        An iterator over the edges of the currently-active graph.
        Each edge is a 3-tuple `(u, v, time)`.
    executor : ThreadPoolExecutor
        A thread pool executor (with `max_workers=1` by default) used to
        load graphs asynchronously in the background.

    Raises
    ------
    StopIteration
        When there are no more graph files and no remaining edges to yield.

    Notes
    -----
    - Each file is loaded asynchronously by `read_graph` in a separate thread.
    - The edges of the currently loaded graph are sorted by their timestamp,
      so we can yield them in ascending time order.
    - Once a file's edges are exhausted, this iterator moves on to the next
      buffer slot (the next graph) and triggers a load of the subsequent file
      if available.
    - If multiple files have overlapping time ranges, only local sorting
      within each file is performed. This class does not perform a global
      merge of overlapping time intervals from multiple files. The assumption
      is each file covers a distinct or mostly chronological partition.
      You may need a different approach if you want a fully global sort
      across all files.

    Example
    -------
    >>> # Suppose you have graph files for a range of dates:
    >>> edge_iter = GraphEdgeIterator(start_date="2023-01-01", end_date="2023-01-31", buffer_count=2)
    >>> for (u, v, t) in edge_iter:
    ...     # Process each edge in ascending time order
    ...     print(u, v, t)
    """

    def __init__(
            self,
            start_date: date | str | None = None,
            end_date: date | str | None = None,
            buffer_count: int = 2,
    ):
        # Prepare an iterator of file paths (graph files) within the date range
        self.graph_path_iterator = iter(get_graph_paths(start_date=start_date, end_date=end_date))

        self.buffer_count = buffer_count
        # Initialize a buffer (list) of size buffer_count for futures or None
        self.buffer = [None] * self.buffer_count

        # Empty iterator for the currently active graph; updated on demand
        self.current_edges: Iterator[tuple[int, int, int]] = iter([])

        # ThreadPoolExecutor with max_workers=1 to asynchronously load the next graph(s)
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=1)

        # Preload the buffers up to the specified buffer_count
        for i in range(min(self.buffer_count, len(self.buffer))):
            self._trigger_buffer(i)

        # Immediately resolve buffer[0] to set up the first graph's edges
        self._resolve_buffer(0)

    def _trigger_buffer(self, index: int):
        """
        Starts loading the next graph file in the background and stores
        the resulting Future in self.buffer[index].

        Parameters
        ----------
        index : int
            Index in the buffer list where the Future will be stored.

        Notes
        -----
        - If the `graph_path_iterator` has been exhausted, this sets
          self.buffer[index] to None instead of a Future.
        """
        try:
            next_graph_path = next(self.graph_path_iterator)  # May raise StopIteration
            future = self.executor.submit(read_graph, next_graph_path)
            self.buffer[index] = future
        except StopIteration:
            # No more graphs to load
            self.buffer[index] = None

    def _resolve_buffer(self, index: int):
        """
        Blocks until the graph in self.buffer[index] is fully loaded (Future resolved),
        then sets the result's edges (sorted by time) as the current_edges iterator.

        Parameters
        ----------
        index : int
            Index in the buffer list from which to retrieve a loaded graph.

        Raises
        ------
        AttributeError
            If the resolved graph object has no `.edges` attribute.
        """
        if self.buffer[index] is None:
            # No future to resolve; implies no more graphs
            self.current_edges = iter([])
            return

        # Wait for the future to complete and get the result
        graph = self.buffer[index].result()
        # Sort the edges by their time (third element of the edge tuple)
        self.current_edges = iter(sorted(graph.edges, key=lambda e: e[2]))

    def __iter__(self) -> "GraphEdgeIterator":
        """
        Returns self as an iterator, enabling usage in a for-loop or any
        iterative context.
        """
        return self

    def __next__(self) -> tuple[int, int, int]:
        """
        Yields the next edge in the current graph. If the current graph is
        exhausted, moves on to the next buffer slot and triggers a load
        for a subsequent graph file (if available).

        Returns
        -------
        (u, v, current_time) : tuple(int, int, int)
            The next edge from the buffered graphs, sorted in ascending
            order by the `current_time` field.

        Raises
        ------
        StopIteration
            If there are no more edges in any of the loaded buffers
            and no more files to load.
        """
        try:
            # Fetch the next edge from the current graph
            return next(self.current_edges)
        except StopIteration:
            # Current graph is exhausted; attempt to move to the next one
            if all(buf is None for buf in self.buffer):
                # If the entire buffer is empty, we're done
                raise StopIteration

            # Shift the buffer contents "to the left"
            for i in range(self.buffer_count - 1):
                self.buffer[i] = self.buffer[i + 1]
            self.buffer[-1] = None

            # Resolve the first buffer slot again to set current_edges
            if self.buffer[0] is not None:
                self._resolve_buffer(0)
            else:
                # If even the first slot is None, no graphs are left
                raise StopIteration

            # Trigger loading of the next graph (if any) in the last slot
            self._trigger_buffer(self.buffer_count - 1)

            # Retry to get the next edge now that buffers have shifted
            return next(self)
