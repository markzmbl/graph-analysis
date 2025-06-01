import csv
import os
from web3 import Web3
from time import sleep
from tqdm import tqdm

# Ethereum node connection (e.g., Infura)
INFURA_URL = "https://mainnet.infura.io/v3/ded1e67464304f9a9b60d4dec0f02985"
web3 = Web3(Web3.HTTPProvider(INFURA_URL))

# Block range
start_block = 13_330_090
end_block = 15_053_226
output_file = "block_timestamps.csv"
request_delay = 0.2  # seconds


def get_last_processed_block(filename: str) -> int:
    """Return the last block number written to the CSV."""
    if not os.path.exists(filename):
        return start_block - 1

    with open(filename, "r") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        last_block = start_block - 1
        for row in reader:
            if row:
                last_block = int(row[0])
        return last_block


def main():
    if not web3.is_connected():
        raise ConnectionError("Web3 provider is not connected.")

    last_block = get_last_processed_block(output_file)
    write_header = not os.path.exists(output_file)

    with open(output_file, mode="a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if write_header:
            writer.writerow(["block_number", "timestamp"])

        block_range = range(last_block + 1, end_block + 1)
        for block_num in tqdm(block_range, desc="Fetching blocks", unit="block"):
            try:
                block = web3.eth.get_block(block_num)
                writer.writerow([block.number, block.timestamp])
                sleep(request_delay)
            except Exception as e:
                tqdm.write(f"Error fetching block {block_num}: {e}")
                sleep(2)


if __name__ == "__main__":
    main()