import asyncio
import pickle

from tqdm.asyncio import tqdm_asyncio
from web3 import AsyncWeb3, AsyncIPCProvider

from input.iterator import get_graph_paths

ERC721_TRANSFER_TOPIC = AsyncWeb3.keccak(text="Transfer(address,address,uint256)").hex()


async def fetch_receipt(web3, edge_id):
    try:
        return await web3.eth.get_transaction_receipt(edge_id)
    except Exception as e:
        print(f"Error fetching receipt {edge_id}: {e}")
        return None


def get_contract_id(receipt):
    for log in receipt.logs:
        topics = log.topics
        if topics and topics[0].lower() == ERC721_TRANSFER_TOPIC.lower():
            return log.address
    return None


async def enrich_edges(graph, web3):
    edge_tasks = []
    edge_lookup = []

    for u, v, block_number, data in graph.edges(keys=True, data=True):
        edge_id = data.get("edge_id")
        if edge_id:
            task = fetch_receipt(web3, edge_id)
            edge_tasks.append(task)
            edge_lookup.append((u, v, block_number))

    receipts = await tqdm_asyncio.gather(*edge_tasks, desc="Collecting transactions/receipts")

    for (u, v, block_number), receipt in zip(edge_lookup, receipts):
        if receipt:
            graph[u][v][block_number]["contract_id"] = get_contract_id(receipt)

    return graph


async def main():
    async with AsyncWeb3(AsyncIPCProvider("~/Library/Ethereum/geth.ipc")) as web3:

        if not await web3.is_connected():
            raise ConnectionError("AsyncWeb3 is not connected to the node.")

        for graph_path in get_graph_paths("2021-10-01", "2022-06-30"):
            with graph_path.open("rb") as fin:
                graph = pickle.load(fin)

            enriched_graph = await enrich_edges(graph, web3)

            with graph_path.with_suffix(".enriched.pickle").open("wb") as fout:
                pickle.dump(enriched_graph, fout)


if __name__ == "__main__":
    asyncio.run(main())