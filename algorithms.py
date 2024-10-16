from collections import defaultdict
from itertools import product

import networkx as nx
from networkx.utils import not_implemented_for


class _NeighborhoodCache(dict):
    """Very lightweight graph wrapper which caches neighborhoods as list.

    This dict subclass uses the __missing__ functionality to query graphs for
    their neighborhoods, and store the result as a list.  This is used to avoid
    the performance penalty incurred by subgraph views.
    """

    def __init__(self, G):
        self.G = G

    def __missing__(self, v):
        Gv = self[v] = list((w, data["time"]) for w, data in self.G[v].items())
        return Gv


# @nx._dispatchable
def chordless_temporal_cycles(graph_df, length_bound=None):
    """Find simple chordless cycles of a graph.

    A `simple cycle` is a closed path where no node appears twice.  In a simple
    cycle, a `chord` is an additional edge between two nodes in the cycle.  A
    `chordless cycle` is a simple cycle without chords.  Said differently, a
    chordless cycle is a cycle C in a graph G where the number of edges in the
    induced graph G[C] is equal to the length of `C`.

    Note that some care must be taken in the case that G is not a simple graph
    nor a simple digraph.  Some authors limit the definition of chordless cycles
    to have a prescribed minimum length; we do not.

        1. We interpret self-loops to be chordless cycles, except in multigraphs
           with multiple loops in parallel.  Likewise, in a chordless cycle of
           length greater than 1, there can be no nodes with self-loops.

        2. We interpret directed two-cycles to be chordless cycles, except in
           multi-digraphs when any edge in a two-cycle has a parallel copy.

        3. We interpret parallel pairs of undirected edges as two-cycles, except
           when a third (or more) parallel edge exists between the two nodes.

        4. Generalizing the above, edges with parallel clones may not occur in
           chordless cycles.

    In a directed graph, two chordless cycles are distinct if they are not
    cyclic permutations of each other.  In an undirected graph, two chordless
    cycles are distinct if they are not cyclic permutations of each other nor of
    the other's reversal.

    Optionally, the cycles are bounded in length.

    We use an algorithm strongly inspired by that of Dias et al [1]_.  It has
    been modified in the following ways:

        1. Recursion is avoided, per Python's limitations

        2. The labeling function is not necessary, because the starting paths
            are chosen (and deleted from the host graph) to prevent multiple
            occurrences of the same path

        3. The search is optionally bounded at a specified length

        4. Support for directed graphs is provided by extending cycles along
            forward edges, and blocking nodes along forward and reverse edges

        5. Support for multigraphs is provided by omitting digons from the set
            of forward edges

    Parameters
    ----------
    G : NetworkX DiGraph
       A directed graph

    length_bound : int or None, optional (default=None)
       If length_bound is an int, generate all simple cycles of G with length at
       most length_bound.  Otherwise, generate all simple cycles of G.

    Yields
    ------
    list of nodes
       Each cycle is represented by a list of nodes along the cycle.

    Examples
    --------
    >>> sorted(list(nx.chordless_cycles(nx.complete_graph(4))))
    [[1, 0, 2], [1, 0, 3], [2, 0, 3], [2, 1, 3]]

    Notes
    -----
    When length_bound is None, and the graph is simple, the time complexity is
    $O((n+e)(c+1))$ for $n$ nodes, $e$ edges and $c$ chordless cycles.

    Raises
    ------
    ValueError
        when length_bound < 0.

    References
    ----------
    .. [1] Efficient enumeration of chordless cycles
       E. Dias and D. Castonguay and H. Longo and W.A.R. Jradi
       https://arxiv.org/abs/1309.1051

    See Also
    --------
    simple_cycles
    """

    if length_bound is not None:
        if length_bound == 0:
            return
        elif length_bound < 0:
            raise ValueError("length bound must be non-negative")


    for u, Gu in G.adj.items():
        Gv = Gu.get(u, ())
        if len(Gv) == 1:
            for data in Gv.values():
                yield [(u, data["time"])]

    if length_bound is not None and length_bound == 1:
        return

    # Nodes with loops cannot belong to longer cycles.  Let's delete them here.
    # also, we implicitly reduce the multiplicity of edges down to 1 in the case
    # of multiedges.
    F = nx.DiGraph()
    for u, Gu in G.adj.items():
        if u not in Gu:
            for v, Gv in Gu.items():
                for key, data in Gv.items():
                    F.add_edge(u, v, key=key, time=data["time"])
    B = F.to_undirected(as_view=False)

    # If we're given a multigraph, we have a few cases to consider with parallel
    # edges.
    #
    # 1. If we have 2 or more edges in parallel between the nodes (u, v), we
    #    must not construct longer cycles along (u, v).
    # 2. If G is not directed, then a pair of parallel edges between (u, v) is a
    #    chordless cycle unless there exists a third (or more) parallel edge.
    # 3. If G is directed, then parallel edges do not form cycles, but do
    #    preclude back-edges from forming cycles (handled in the next section),
    #    Thus, if an edge (u, v) is duplicated and the reverse (v, u) is also
    #    present, then we remove both from F.
    #
    # In directed graphs, we need to consider both directions that edges can
    # take, so iterate over all edges (u, v) and possibly (v, u).  In undirected
    # graphs, we need to be a little careful to only consider every edge once,
    # so we use a "visited" set to emulate node-order comparisons.

    # If we're given a directed graphs, we need to think about digons.  If we
    # have two edges (u, v) and (v, u), then that's a two-cycle.  If either edge
    # was duplicated above, then we removed both from F.  So, any digons we find
    # here are chordless.  After finding digons, we remove their edges from F
    # to avoid traversing them in the search for chordless cycles.
    for u, Fu in F.adj.items():
        digons = [[u, v] for v in Fu if F.has_edge(v, u)]
        yield from digons
        F.remove_edges_from(digons)
        F.remove_edges_from(e[::-1] for e in digons)

    if length_bound is not None and length_bound == 2:
        return

    # Now, we prepare to search for cycles.  We have removed all cycles of
    # lengths 1 and 2, so F is a simple graph or simple digraph.  We repeatedly
    # separate digraphs into their strongly connected components, and undirected
    # graphs into their biconnected components.  For each component, we pick a
    # node v, search for chordless cycles based at each "stem" (u, v, w), and
    # then remove v from that component before separating the graph again.
    separate = nx.strongly_connected_components

    # Directed stems look like (u -> v -> w), so we use the product of
    # predecessors of v with successors of v.
    def stems(C, v):
        for (u, data_u), (w, data_w) in product(C.pred[v].items(), C.succ[v].items()):
            if not G.has_edge(u, w):  # omit stems with acyclic chords
                yield [u, v, w], F.has_edge(w, u)

    components = [c for c in separate(F) if len(c) > 2]
    while components:
        c = components.pop()
        v = next(iter(c))
        Fc = F.subgraph(c)
        Fcc = Bcc = None
        for S, is_triangle in stems(Fc, v):
            if is_triangle:
                yield S
            else:
                if Fcc is None:
                    Fcc = _NeighborhoodCache(Fc)
                    Bcc = Fcc if B is None else _NeighborhoodCache(B.subgraph(c))
                yield from _chordless_cycle_search(Fcc, Bcc, S, length_bound)

        components.extend(c for c in separate(F.subgraph(c - {v})) if len(c) > 2)


def _chordless_cycle_search(F, B, path, length_bound):
    """The main loop for chordless cycle enumeration.

    This algorithm is strongly inspired by that of Dias et al [1]_.  It has been
    modified in the following ways:

        1. Recursion is avoided, per Python's limitations

        2. The labeling function is not necessary, because the starting paths
            are chosen (and deleted from the host graph) to prevent multiple
            occurrences of the same path

        3. The search is optionally bounded at a specified length

        4. Support for directed graphs is provided by extending cycles along
            forward edges, and blocking nodes along forward and reverse edges

        5. Support for multigraphs is provided by omitting digons from the set
            of forward edges

    Parameters
    ----------
    F : _NeighborhoodCache
       A graph of forward edges to follow in constructing cycles

    B : _NeighborhoodCache
       A graph of blocking edges to prevent the production of chordless cycles

    path : list
       A cycle prefix.  All cycles generated will begin with this prefix.

    length_bound : int
       A length bound.  All cycles generated will have length at most length_bound.


    Yields
    ------
    list of nodes
       Each cycle is represented by a list of nodes along the cycle.

    References
    ----------
    .. [1] Efficient enumeration of chordless cycles
       E. Dias and D. Castonguay and H. Longo and W.A.R. Jradi
       https://arxiv.org/abs/1309.1051

    """
    blocked = defaultdict(int)
    target = path[0]
    blocked[path[1]] = 1
    for w in path[1:]:
        for v in B[w]:
            blocked[v] += 1

    stack = [iter(F[path[2]])]
    while stack:
        nbrs = stack[-1]
        for w in nbrs:
            if blocked[w] == 1 and (length_bound is None or len(path) < length_bound):
                Fw = F[w]
                if target in Fw:
                    yield path + [w]
                else:
                    Bw = B[w]
                    if target in Bw:
                        continue
                    for v in Bw:
                        blocked[v] += 1
                    path.append(w)
                    stack.append(iter(Fw))
                    break
        else:
            stack.pop()
            for v in B[path.pop()]:
                blocked[v] -= 1
