import pickle

import networkx as nx
from tqdm import tqdm

from utils import iteration_logging, get_drive
from pathlib import Path

drive = get_drive()

graph_paths = sorted((p for p in (drive / "graphs").iterdir() if p.suffix == ".pickle"),
                     key=lambda x: x.stem.split("__")[-1])

for length_bound in (3, 5, 7):
    log_directory = drive / f"simple_cycles/{length_bound}"
    log_directory.mkdir(parents=True, exist_ok=True)
    for graph_path in tqdm(graph_paths, desc=f"{length_bound=}"):
        log_path = log_directory / f"{graph_path.stem}.csv"
        with graph_path.open("rb") as graph_file:
            graph = pickle.load(graph_file)
        for cycle in tqdm(iteration_logging(nx.simple_cycles(graph, length_bound=length_bound), log_path),
                          desc=graph.stem):
            pass
