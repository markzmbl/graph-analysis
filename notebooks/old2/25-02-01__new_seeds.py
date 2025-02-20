#%%
from utils import iteration_logging
from cycles.graph_cycle_iterator import GraphCycleIterator
from graph_edge_iterator import GraphEdgeIterator
from tqdm.auto import tqdm

#%%
BUFFER_COUNT = 2
PRUNE_INTERVAL = 1_000
END_DATE = "2019-08-10"
OMEGA = 10
#%%
def edge_generator():
    return GraphEdgeIterator(end_date=END_DATE, buffer_count=BUFFER_COUNT)

TOTAL_EDGES = len(list(edge_generator()))
#%%
def wrapped_edge_generator(log_stream):
    return tqdm(
        iteration_logging(
            edge_generator(),
            log_stream=log_stream
        ),
        total=TOTAL_EDGES
    )


def cycle_generator(omega, log_stream_edges=None, combine_seeds=True, track_history=False):
    return GraphCycleIterator(
        wrapped_edge_generator(log_stream_edges),
        omega,
        combine_seeds=combine_seeds,
        track_history=track_history
    )
#%%
iterator = cycle_generator(OMEGA, combine_seeds=False, track_history=False)
list(iterator)
#%%
