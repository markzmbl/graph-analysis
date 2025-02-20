#%%

from utils import iteration_logging
from cycles.graph_cycle_iterator import GraphCycleIterator
from graph_edge_iterator import GraphEdgeIterator
from tqdm.auto import tqdm
import io
import pandas as pd
#%%
if __name__ == "__main__":
    OMEGA = 10
    BUFFER_COUNT = 2
    PRUNE_INTERVAL = 1_000
    END_DATE = "2019-08-10"
    TOTAL_EDGES = len(list(GraphEdgeIterator(end_date=END_DATE, buffer_count=BUFFER_COUNT)))
    #%%

    csv_stream = io.StringIO()

    wrapped_edges = GraphCycleIterator(
        tqdm(  # type: ignore
            iteration_logging(
                GraphEdgeIterator(end_date=END_DATE, buffer_count=BUFFER_COUNT),
                log_stream=csv_stream,
            ),
            total=TOTAL_EDGES,
            desc="Edges",
        ),
        omega=OMEGA,
        prune_interval=PRUNE_INTERVAL
    )

    wrapped_edges.run()


    #%%
    csv_stream.seek(0)
    df = pd.read_csv(csv_stream)
    df = df.set_index("time_seconds")
    df.plot(subplots=True)
    #%%
    sum(len(intervals) for intervals in wrapped_edges._seed_intervals.values())
    #%%
