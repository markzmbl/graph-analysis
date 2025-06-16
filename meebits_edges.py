import pandas as pd
from tqdm import tqdm

from input.iterator import GraphEdgeIterator

df = pd.read_csv("export-nft-trades.csv")
df["Transaction Hash"] = df["Transaction Hash"].str.lower().apply(lambda x: x.removeprefix("0x"))
df.set_index("Transaction Hash", inplace=True)

for u, v, block_number, data in tqdm(GraphEdgeIterator("2021-10-01", "2022-06-30")):
    tx_hash = data["edge_id"].hex()
    if tx_hash in df.index:
        df.loc[tx_hash, ["Source Index", "Target Index", "Source ID", "Target ID", "Value"]] = [
            u, v, data["source_id"].hex(), data["target_id"].hex(), data["value"]
        ]
        print(df.loc[tx_hash].to_dict())

df.to_csv("export-nft-trades2.csv")