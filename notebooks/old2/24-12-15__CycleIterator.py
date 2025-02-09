from utils import GraphEdgeIterator, GraphCycleIterator, iteration_logging
from tqdm.auto import tqdm
import io
import pandas as pd

OMEGA = 10
BUFFER_COUNT = 2
PRUNE_INTERVAL = 1_000

_TOTAL_EDGES = 2_912_276

csv_stream = io.StringIO()

wrapped_edges = GraphCycleIterator(
    tqdm(
        iteration_logging(
            GraphEdgeIterator(buffer_count=BUFFER_COUNT),
            log_stream=csv_stream,
        ),
        total=_TOTAL_EDGES,
        desc="Edges",
    ),
    omega=OMEGA,
    prune_interval=PRUNE_INTERVAL
)

wrapped_edges.run()
df = pd.read_csv(csv_stream)
df = df.set_index("time_seconds")
df.plot(subplots=True)
