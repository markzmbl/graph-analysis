#%%
from utils import GraphEdgeIterator
from tqdm.notebook import tqdm
#%%
def generate_seeds(edges, omega, prune_interval=1_000):
    """
    
    :param edges: 
    :param omega: 
    :param prune_interval: 
    :return: All vertexs s, current_timestamp stamps t_start and t_end, and a candidates set such that there exists a loop from s to s
        using only vertexs in C starting at t_start and ending at t_end
    """
    S = {}  # Summaries for each root

    for i, (a, b, t) in enumerate(edges):
        if a == b:
            continue

        # Ensure S(b) exists
        if b not in S:
            S[b] = set()
        # Add (a, t) to S(b)
        S[b].add((a, t))

        if a in S:
            # Prune outdated entries from S(a)
            S[a] = {(x, tx) for (x, tx) in S[a] if tx > t - omega}
            # Update S(b) with entries from S(a)
            S[b].update(S[a])

            # Iterate over copies to avoid modification issues
            for vertex_b, tb in list(S[b]):
                if vertex_b == b:
                    # Construct candidates C
                    candidates = {c for (c, tc) in S[a] if tc > tb}
                    # Output the seed
                    yield b, (tb, t), candidates
                    # Remove (b, tb) from S(b)
                    S[b].remove((b, tb))

        # Time to prune all summaries
        if i % prune_interval == 0:
            for vertex in S:
                S[vertex] = {(y, ty) for (y, ty) in S[vertex] if ty > t - omega}
#%%
edges = list(GraphEdgeIterator(end_date="2019-08-14"))

#%%
omega = 10  # Threshold
#%%
# pbar = tqdm(edges)
list(generate_seeds(edges, omega))
#%%
