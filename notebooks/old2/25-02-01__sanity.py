from cycles.iterator import TransactionGraph
# Example usage
graph = TransactionGraph()
graph.add_edge(1, 2, key=10)
graph.add_edge(2, 3, key=20)
graph.add_edge(3, 4, key=30)

# time_filtered_graph = graph.time_slice(begin=15, end=31)
# graph.prune(_lower_time_limit=10)
print(graph.length())