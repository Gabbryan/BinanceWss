import json
import logging
import os
import threading
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from requests.exceptions import HTTPError
from tqdm import tqdm
from web3 import Web3

# Set up logging
logging.basicConfig(
    filename="script.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w",
)

# List of Ethereum RPC endpoints
endpoints = [
    "https://ethereum-rpc.publicnode.com",
]


# Function to create a Web3 instance with a given RPC endpoint
def create_web3_instance(endpoint):
    logging.debug(f"Creating Web3 instance with endpoint: {endpoint}")
    return Web3(Web3.HTTPProvider(endpoint))


# Initialize Web3 with the first endpoint
web3_index = 0
web3 = create_web3_instance(endpoints[web3_index])

# ERC-20 Token ABI (simplified)
erc20_abi = json.loads(
    """
[
    {"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},
    {"constant":false,"inputs":[{"name":"_to","type":"address"},{"name":"_value","type":"uint256"}],"name":"transfer","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},
    {"anonymous":false,"inputs":[{"indexed":true,"name":"from","type":"address"},{"indexed":true,"name":"to","type":"address"},{"indexed":false,"name":"value","type":"uint256"}],"name":"Transfer","type":"event"}
]
"""
)

# Stablecoin contract address (e.g., USDT)
usdt_address = web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7")
logging.debug(f"Using USDT contract address: {usdt_address}")

# Define the contract
token_contract = web3.eth.contract(address=usdt_address, abi=erc20_abi)


def switch_provider():
    global web3, web3_index
    web3_index = (web3_index + 1) % len(endpoints)
    web3 = create_web3_instance(endpoints[web3_index])
    logging.info(f"Switched to RPC endpoint: {endpoints[web3_index]}")


def fetch_logs(start_block, end_block):
    max_retries = 5
    delay = 1  # Initial delay in seconds
    logging.debug(f"Fetching logs from block {start_block} to {end_block}")

    for attempt in range(max_retries):
        try:
            filter_params = {
                "fromBlock": start_block,
                "toBlock": end_block,
                "address": usdt_address,
                "topics": [web3.keccak(text="Transfer(address,address,uint256)").hex()],
            }
            logs = web3.eth.get_logs(filter_params)
            logging.info(
                f"Fetched {len(logs)} logs from block range {start_block} to {end_block}"
            )
            return logs
        except HTTPError as e:
            if e.response.status_code == 429:  # Rate limit error
                logging.warning(
                    f"Rate limit exceeded at endpoint {endpoints[web3_index]}. Attempt {attempt + 1}/{max_retries}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logging.error(f"HTTPError: {e}")
                return []
        except Exception as e:
            logging.error(f"Error fetching logs: {e}")
            return []

    logging.error("Max retries reached. Skipping this block range.")
    return []


def decode_log(log):
    try:
        topics = log["topics"]
        data = log["data"]

        from_address = Web3.to_checksum_address(Web3.to_hex(topics[1])[26:])
        to_address = Web3.to_checksum_address(Web3.to_hex(topics[2])[26:])
        value_hex = Web3.to_hex(data)[2:].zfill(64)
        value = int(value_hex, 16) / 10**5

        block_number = log["blockNumber"]
        block = web3.eth.get_block(block_number, full_transactions=False)
        timestamp = pd.to_datetime(block["timestamp"], unit="s")

        log_data = {
            "from": from_address,
            "to": to_address,
            "value": value,
            "timestamp": timestamp,
        }
        logging.debug(f"Decoded log: {log_data}")

        # Save the decoded log to Parquet immediately
        save_to_parquet(log_data)

        return log_data
    except Exception as e:
        logging.error(f"Error decoding log: {e}")
        return None


def save_to_parquet(log_data):
    df = pd.DataFrame([log_data])

    timestamp = log_data["timestamp"]
    year = timestamp.year
    month = timestamp.month
    day = timestamp.day

    output_directory = "OUTPUT"  # Base directory for output files
    directory = os.path.join(output_directory, f"{year}/{month:02d}/{day:02d}")
    os.makedirs(directory, exist_ok=True)

    file_path = os.path.join(directory, "data.parquet")

    # Load existing data if file exists
    if os.path.exists(file_path):
        existing_df = pd.read_parquet(file_path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df

    # Write data to Parquet
    table = pa.Table.from_pandas(combined_df)
    pq.write_table(table, file_path)

    logging.info(f"Saved data to Parquet file: {file_path}")


def process_block_range(start_block, end_block):
    logging.info(
        f"Processing block range from {start_block} to {end_block} block by block"
    )

    total_blocks = end_block - start_block + 1

    # Initialize tqdm progress bar
    with tqdm(total=total_blocks, desc="Processing Blocks") as bar:
        for block_number in range(start_block, end_block + 1):
            logging.debug(f"Processing block {block_number}")
            logs = fetch_logs(block_number, block_number)
            if not logs:  # If no logs are fetched, switch endpoint and retry
                switch_provider()
                logs = fetch_logs(block_number, block_number)
            for log in logs:
                decode_log(log)
            bar.update(1)

            # Save progress after processing each block
            save_last_processed_block(block_number)

    logging.info(f"Processed logs for block range {start_block}-{end_block}")


def thread_worker(start_block, end_block):
    logging.info(f"Starting thread for block range {start_block} to {end_block}")
    process_block_range(start_block, end_block)
    logging.info(f"Finished thread for block range {start_block} to {end_block}")


def save_last_processed_block(block_number, filename="last_processed_block.csv"):
    df = pd.DataFrame([{"last_processed_block": block_number}])
    df.to_csv(filename, index=False)
    logging.info(f"Saved last processed block: {block_number}")


def load_last_processed_block(filename="last_processed_block.csv"):
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        block_number = int(df["last_processed_block"].iloc[-1])
        logging.info(f"Loaded last processed block: {block_number}")
        return block_number
    else:
        logging.info("No last processed block found, starting from the beginning.")
        return None


def main():
    # Load the last processed block if it exists
    last_processed_block = load_last_processed_block()

    # Set the block number for 1st January 2023
    start_block = (
        last_processed_block if last_processed_block else 16308683
    )  # Use saved block or default starting block
    end_block = (
        20544033  # Example ending block (current block number or some other end block)
    )
    logging.info(f"Processing block range from {start_block} to {end_block}")
    # Set up threading
    threads = []
    num_threads = 2  # Adjust based on your needs and rate limits
    block_ranges = [
        (
            start_block + i * (end_block - start_block) // num_threads,
            start_block + (i + 1) * (end_block - start_block) // num_threads - 1,
        )
        for i in range(num_threads)
    ]

    logging.info(f"Starting {num_threads} threads for processing block ranges.")
    for start, end in block_ranges:
        thread = threading.Thread(target=thread_worker, args=(start, end))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    logging.info("All threads finished.")


if __name__ == "__main__":
    main()
