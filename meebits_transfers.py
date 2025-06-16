import requests
import time
import csv
from datetime import datetime, timedelta, UTC
from tqdm import tqdm

API_KEY = 'DU63XUHFDVA9B3A3XQ3F64PR61E1BNM3W8'
BASE_URL = 'https://api.etherscan.io/api'
ADDRESS = '0x7Bd29408f11D2bFC23c34f18275bBf23bB716Bc7'
OUTPUT_CSV = 'transactions.csv'


def fetch_transactions(start_block=13_330_090, end_block=22_637_659, page=1, offset=100):
    params = {
        'module': 'account',
        'action': 'tokennfttx',
        'contractaddress': ADDRESS,
        'startblock': start_block,
        'endblock': end_block,
        'page': page,
        'offset': offset,
        'sort': 'asc',
        'apikey': API_KEY
    }

    while True:
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            data = response.json()
            if data['status'] == '1':
                yield from data['result']
                if len(data['result']) < offset:
                    break
                else:
                    page += 1
                    params['page'] = page
            elif data['status'] == '0' and data['message'] == 'No transactions found':
                break
            else:
                print(f"Error: {data['message']}")
                break
        elif response.status_code == 429:
            # Rate limit exceeded
            now = datetime.now()
            next_day = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            sleep_seconds = (next_day - now).total_seconds()
            print(f"Rate limit exceeded. Sleeping until next day ({next_day}).")
            time.sleep(sleep_seconds)
        else:
            print(f"HTTP Error: {response.status_code}")
            break


def process_transactions():
    fieldnames = ['blockNumber', 'timeStamp', 'hash', 'from', 'to', 'tokenID']
    with open(OUTPUT_CSV, mode='w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        transactions = fetch_transactions()
        progress_bar = tqdm(transactions, desc='Processing transactions', unit='tx')

        for tx in progress_bar:
            writer.writerow({field: tx.get(field, '') for field in fieldnames})
            # Update progress bar description with the latest timestamp
            timestamp = int(tx.get('timeStamp', '0'))
            formatted_time = datetime.fromtimestamp(timestamp, UTC).strftime('%Y-%m-%d %H:%M:%S')
            progress_bar.set_description(f"Last timestamp: {formatted_time}")
            yield tx


if __name__ == '__main__':
    for transaction in process_transactions():
        # You can process each transaction here
        pass
