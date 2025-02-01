import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

def bin_lengths(lengths, bin_edges):
    """Bins length counts into predefined bins."""
    binned_counts = {f"{bin_edges[i]}-{bin_edges[i+1]}": 0 for i in range(len(bin_edges) - 1)}
    for length, count in lengths.items():
        for i in range(len(bin_edges) - 1):
            if bin_edges[i] <= length < bin_edges[i + 1]:
                binned_counts[f"{bin_edges[i]}-{bin_edges[i+1]}"] += count
                break
    return binned_counts

def process_data_for_plot(data):
    """Converts list of length-count dictionaries into a DataFrame for plotting."""
    all_lengths = set()
    for d in data:
        all_lengths.update(d.keys())

    bin_edges = compute_bin_edges(all_lengths)

    # Re-bin the data
    binned_data = [bin_lengths(d, bin_edges) for d in data]

    # Convert to DataFrame
    df = pd.DataFrame(binned_data).fillna(0)
    return df


def compute_bin_edges(lengths):
    """Computes sensible bin edges for histogram plotting."""
    if not lengths:
        return []

    min_len, max_len = min(lengths), max(lengths)

    if max_len - min_len > 20:
        return np.unique(np.logspace(np.log10(min_len), np.log10(max_len), num=10, dtype=int))

    return np.arange(min_len, max_len + 5, step=max((max_len - min_len) // 10, 1))


def process_data_for_hist(data):
    """
    Converts a length-count dictionary into a list of values
    suitable for plt.hist and computes bin edges.
    """
    all_values = [length for length, count in data.items() for _ in range(count)]
    bin_edges = compute_bin_edges(set(data.keys()))

    return all_values, bin_edges

def plot_stacked_area(df, title=None):
    """Plots a stacked area chart from the processed DataFrame."""
    df.plot(kind='area', stacked=True, colormap='viridis', alpha=0.7, figsize=(10, 6))
    plt.xlabel("Index")
    plt.ylabel("Count")
    plt.title("Stacked Area Chart of Binned Length Frequencies")
    plt.legend(title="Bins", bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True, linestyle='--', alpha=0.5)

    if title is not None:
        plt.title(title)
    plt.show()
