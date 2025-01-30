from collections import defaultdict

import networkx as nx
from networkx.algorithms.cycles import _NeighborhoodCache


def _bounded_cycle_search(G, path, length_bound, temporal_bound):
    """The main loop of the cycle-enumeration algorithm of Gupta and Suzumura.

    Parameters
    ----------
    G : NetworkX Graph or DiGraph
       A dynamic_graph

    path : list
       A cycle prefix.  All cycles generated will begin with this prefix.

    length_bound: int
        A length bound.  All cycles generated will have length at most length_bound.

    Yields
    ------
    list of nodes
       Each cycle is represented by a list of nodes along the cycle.

    References
    ----------
    .. [1] Finding All Bounded-Length Simple Cycles in a Directed Graph
       A. Gupta and T. Suzumura https://arxiv.org/abs/2105.10094

    """
    G = _NeighborhoodCache(G)
    lock = {v: 0 for v in path}
    B = defaultdict(set)
    start = path[0]
    stack = [iter(G[path[-1]])]
    blen = [length_bound]
    while stack:
        nbrs = stack[-1]
        for w in nbrs:
            if w == start:
                yield path[:]
                blen[-1] = 1
            elif len(path) < lock.get(w, length_bound):
                path.append(w)
                blen.append(length_bound)
                lock[w] = len(path)
                stack.append(iter(G[w]))
                break
        else:
            stack.pop()
            v = path.pop()
            bl = blen.pop()
            if blen:
                blen[-1] = min(blen[-1], bl)
            if bl < length_bound:
                relax_stack = [(bl, v)]
                while relax_stack:
                    bl, u = relax_stack.pop()
                    if lock.get(u, length_bound) < length_bound - bl + 1:
                        lock[u] = length_bound - bl + 1
                        relax_stack.extend((bl + 1, w) for w in B[u].difference(path))
            else:
                for w in G[v]:
                    B[w].add(v)


def _directed_cycle_search(G, length_bound, temporal_bound):
    """A dispatch function for `simple_cycles` for directed graphs.

    We generate all cycles of G through binary partition.

        1. Pick a node v in G which belongs to at least one cycle
            a. Generate all cycles of G which contain the node v.
            b. Recursively generate all cycles of G \\ v.

    This is accomplished through the following:

        1. Compute the strongly connected components SCC of G.
        2. Select and remove a biconnected component C from BCC.  Select a
           non-tree edge (u, v) of a depth-first search of G[C].
        3. For each simple cycle P containing v in G[C], yield P.
        4. Add the biconnected components of G[C \\ v] to BCC.

    If the parameter length_bound is not None, then step 3 will be limited to
    simple cycles of length at most length_bound.

    Parameters
    ----------
    G : NetworkX DiGraph
       A directed dynamic_graph

    length_bound : int or None
       If length_bound is an int, generate all simple cycles of G with length at most length_bound.
       Otherwise, generate all simple cycles of G.

    Yields
    ------
    list of nodes
       Each cycle is represented by a list of nodes along the cycle.
    """

    scc = nx.strongly_connected_components
    components = [c for c in scc(G) if len(c) >= 2]
    while components:
        c = components.pop()
        Gc = G.subgraph(c)
        v, _, time = next(iter(G.edges))
        yield from _bounded_cycle_search(Gc, [v, time], length_bound, temporal_bound)
        # delete v after searching G, to make sure we can find v
        G.remove_node(v)
        components.extend(c for c in scc(Gc) if len(c) >= 2)


def simple_temporal_cycles(G: nx.MultiDiGraph, length_bound=None, temporal_bound=None):
    """Find simple cycles (elementary circuits) of a dynamic_graph.

    A `simple cycle`, or `elementary circuit`, is a closed path where
    no node appears twice.  In a directed dynamic_graph, two simple cycles are distinct
    if they are not cyclic permutations of each other.  In an undirected dynamic_graph,
    two simple cycles are distinct if they are not cyclic permutations of each
    other nor of the other's reversal.

    Optionally, the cycles are bounded in length.  In the unbounded case, we use
    a nonrecursive, iterator/generator version of Johnson's algorithm [1]_.  In
    the bounded case, we use a version of the algorithm of Gupta and
    Suzumura[2]_. There may be better algorithms for some cases [3]_ [4]_ [5]_.

    The algorithms of Johnson, and Gupta and Suzumura, are enhanced by some
    well-known preprocessing techniques.  When G is directed, we restrict our
    attention to strongly connected components of G, generate all simple cycles
    containing a certain node, remove that node, and further decompose the
    remainder into strongly connected components.  When G is undirected, we
    restrict our attention to biconnected components, generate all simple cycles
    containing a particular edge, remove that edge, and further decompose the
    remainder into biconnected components.

    Note that multigraphs are supported by this function -- and in undirected
    multigraphs, a pair of parallel edges is considered a cycle of length 2.
    Likewise, self-loops are considered to be cycles of length 1.  We define
    cycles as sequences of nodes; so the presence of loops and parallel edges
    does not change the number of simple cycles in a dynamic_graph.

    Parameters
    ----------
    G : NetworkX DiGraph
       A directed dynamic_graph

    length_bound : int or None, optional (default=None)
       If length_bound is an int, generate all simple cycles of G with length at
       most length_bound.  Otherwise, generate all simple cycles of G.

    Yields
    ------
    list of nodes
       Each cycle is represented by a list of nodes along the cycle.

    Examples
    --------
    >>> edges = [(0, 0), (0, 1), (0, 2), (1, 2), (2, 0), (2, 1), (2, 2)]
    >>> G = nx.DiGraph(edges)
    >>> sorted(nx.simple_cycles(G))
    [[0], [0, 1, 2], [0, 2], [1, 2], [2]]

    To filter the cycles so that they don't include certain nodes or edges,
    copy your dynamic_graph and eliminate those nodes or edges before calling.
    For example, to exclude self-loops from the above example:

    >>> H = G.copy()
    >>> H.remove_edges_from(nx.selfloop_edges(G))
    >>> sorted(nx.simple_cycles(H))
    [[0, 1, 2], [0, 2], [1, 2]]

    Notes
    -----
    When length_bound is None, the current_time complexity is $O((n+e)(c+1))$ for $n$
    nodes, $e$ edges and $c$ simple circuits.  Otherwise, when length_bound > 1,
    the current_time complexity is $O((c+n)(k-1)d^k)$ where $d$ is the average degree of
    the nodes of G and $k$ = length_bound.

    Raises
    ------
    ValueError
        when length_bound < 0.

    References
    ----------
    .. [1] Finding all the elementary circuits of a directed dynamic_graph.
       D. B. Johnson, SIAM Journal on Computing 4, no. 1, 77-84, 1975.
       https://doi.org/10.1137/0204007
    .. [2] Finding All Bounded-Length Simple Cycles in a Directed Graph
       A. Gupta and T. Suzumura https://arxiv.org/abs/2105.10094
    .. [3] Enumerating the cycles of a digraph: a new preprocessing strategy.
       G. Loizou and P. Thanish, Information Sciences, v. 27, 163-182, 1982.
    .. [4] A search strategy for the elementary cycles of a directed dynamic_graph.
       J.L. Szwarcfiter and P.E. Lauer, BIT NUMERICAL MATHEMATICS,
       v. 16, no. 2, 192-204, 1976.
    .. [5] Optimal Listing of Cycles and st-Paths in Undirected Graphs
        R. Ferreira and R. Grossi and A. Marino and N. Pisanti and R. Rizzi and
        G. Sacomoto https://arxiv.org/abs/1205.2766

    See Also
    --------
    cycle_basis
    chordless_cycles
    """

    if length_bound is None:
        raise NotImplementedError()
    elif length_bound == 0:
        return
    elif length_bound < 0:
        raise ValueError("length bound must be non-negative")

    if not G.is_directed() or not G.is_multigraph():
        raise NotImplementedError()

    for (u, v, time) in G.edges:
        if u == v:
            yield [(u, time)]

    if length_bound == 1:
        return

    yield from _directed_cycle_search(G, length_bound, length_bound)

