import configparser
import json
import logging
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import websocket
from dotenv import load_dotenv
from slack_package import init_slack, get_slack_decorators

# Constants for paths
CONFIG_PATH = "../src/cores/cicada-ingestion/data-providers/Binance/sources/ingestion-live-wss/config.conf"
LOG_DIR = "../src/cores/cicada-ingestion/data-providers/Binance/sources/ingestion-live-wss/logs"
load_dotenv()
BEARER = os.getenv("BEARER_TOKEN")
EXCHANGE = os.getenv("EXCHANGE", "Binance").lower()
WEBSOCKET_URLS = {"Binance": "wss://fstream.Binance.com/ws"}
WEBHOOK_URL = os.getenv("WEBHOOK_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME")
S3_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")

# Check if WEBHOOK_URL is defined
if (
    not WEBHOOK_URL
    or WEBHOOK_URL == "https://hooks.slack.com/services/your/webhook/url"
):
    raise ValueError(
        "WEBHOOK_URL is not defined or invalid. Please set it in your .env file."
    )

# Global variables for tracking request times
request_times = defaultdict(list)
total_requests = 0

# Initialize Slack channel
slack_channel = init_slack(WEBHOOK_URL)
slack_decorators = get_slack_decorators()
config = configparser.ConfigParser()
config.read(CONFIG_PATH)

streams = config["settings"]["streams"].split(",")
cryptos = config["settings"]["cryptos"].split(",")


def save_dataframe_to_s3(df, exchange, symbol, day, hour, minute, data_type):
    s3_path = f"s3://{S3_BUCKET_NAME}/cicada-data/{data_type}/live/ingestion/{exchange}/{symbol}/{day}/{hour}/{minute}/data.parquet"
    fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)

    # Read existing data if the file exists
    if fs.exists(s3_path):
        existing_table = pq.read_table(s3_path, filesystem=fs)
        new_table = pa.Table.from_pandas(df)
        combined_table = pa.concat_tables([existing_table, new_table])
    else:
        combined_table = pa.Table.from_pandas(df)

    pq.write_table(combined_table, s3_path, filesystem=fs)
    print(f"DataFrame has been uploaded directly to S3 path: {s3_path}")
    return s3_path


def post_market_data(message_data, symbol, data_type):
    global total_requests, request_times

    # Determine the current time for organizing files
    now = datetime.utcnow()
    day_str = now.strftime("%Y-%m-%d")
    hour_str = now.strftime("%H")
    minute_str = now.strftime("%M")

    table = None

    # Construct the data to be written
    try:
        if data_type == "aggTrade":
            data = {
                "market_raw_id": [1],
                "price": [float(message_data["p"])],
                "quantity": [float(message_data["q"])],
                "timestamp": [float(message_data["E"])],
                "is_buyer_market": [bool(message_data["m"])],
                "is_best_match": [bool(0)],
                "day": [day_str],
                "hour": [hour_str],
                "minute": [minute_str],
            }
        elif data_type == "forceOrder":
            data = {
                "event_type": [message_data["e"]],
                "event_time": [message_data["E"]],
                "symbol": [message_data["o"]["s"]],
                "side": [message_data["o"]["S"]],
                "order_type": [message_data["o"]["o"]],
                "time_in_force": [message_data["o"]["f"]],
                "original_quantity": [float(message_data["o"]["q"])],
                "price": [float(message_data["o"]["p"])],
                "average_price": [float(message_data["o"]["ap"])],
                "order_status": [message_data["o"]["X"]],
                "order_last_filled_quantity": [float(message_data["o"]["l"])],
                "order_filled_accumulated_quantity": [float(message_data["o"]["z"])],
                "order_trade_time": [message_data["o"]["T"]],
                "day": [day_str],
                "hour": [hour_str],
                "minute": [minute_str],
            }

        table = pa.Table.from_pydict(data)
    except KeyError as e:
        logging.error(f"Key error: {e} in message_data: {message_data}")
        return

    # Write the data to a Parquet file and upload to S3
    if table is not None:
        # Convert to DataFrame and save to S3
        df = table.to_pandas()
        save_dataframe_to_s3(
            df, EXCHANGE, symbol, day_str, hour_str, minute_str, data_type
        )

    total_requests += 1
    request_times[day_str].append(now.timestamp())
    save_request_times(day_str)


def save_request_times(day):
    file_path = os.path.join(LOG_DIR, f"request_times_{day}.json")
    with open(file_path, "w") as file:
        json.dump(request_times[day], file)


def load_request_times(day):
    file_path = os.path.join(LOG_DIR, f"request_times_{day}.json")
    if os.path.exists(file_path):
        with open(file_path, "r") as file:
            request_times[day] = json.load(file)


def on_message(ws, message, symbol, data_type):
    try:
        message_data = json.loads(message)
        print(f"Received message: {message_data}")
        post_market_data(message_data, symbol, data_type)
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e} with message: {message}")
    except Exception as e:
        logging.error(f"Unexpected error: {e} with message: {message}")


def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")
    ws.close()
    time.sleep(0.1)
    ws.run_forever(ping_interval=60, ping_timeout=10)


def on_close(ws, close_status_code, close_msg):
    logging.error(f"WebSocket closed: {close_status_code} - {close_msg}")


def on_ping(ws, message):
    logging.info("Ping sent")


def on_pong(ws, message):
    logging.info("Pong received")


def websocket_thread(symbol, data_type):
    while True:
        ws = websocket.WebSocketApp(
            f"{WEBSOCKET_URLS[EXCHANGE]}/{symbol}@{data_type}",
            on_message=lambda ws, msg: on_message(ws, msg, symbol, data_type),
            on_error=on_error,
            on_close=on_close,
            on_ping=on_ping,
            on_pong=on_pong,
        )
        logging.info(f"WebSocket connection established for {symbol} {data_type}")
        ws.run_forever(ping_interval=60, ping_timeout=10)
        logging.error(
            f"WebSocket connection closed for {symbol} {data_type}. Reconnecting..."
        )


def setup_directories():
    os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{LOG_DIR}/log_ws.log"),
        ],
    )


def analysis_scheduler():
    while True:
        now = datetime.utcnow()
        next_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
        next_midnight = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)

        if now < next_noon:
            next_run = next_noon
        elif now < next_midnight:
            next_run = next_midnight
        else:
            next_run = next_noon + timedelta(days=1)

        wait_seconds = (next_run - now).total_seconds()
        slack_channel.send_message(
            "Analysis Scheduling", f"Next analysis scheduled at {next_run} UTC"
        )
        time.sleep(wait_seconds)
        day = now.strftime("%Y-%m-%d")

        # Log summary of request times every 24 hours
        if day in request_times:
            num_requests = len(request_times[day])
            logging.info(f"{num_requests} requests made in the last 24 hours for {day}")


def periodic_log():
    while True:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Service is running. Current time: {now}")
        time.sleep(60)


def main():
    setup_directories()
    setup_logging()
    logging.info("Service is starting...")

    # Start the analysis scheduler in a separate thread
    scheduler_thread = threading.Thread(target=analysis_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()

    # Start periodic logging thread
    log_thread = threading.Thread(target=periodic_log)
    log_thread.daemon = True
    log_thread.start()

    # Start WebSocket threads for each cryptocurrency and the specified data types
    for crypto in cryptos:
        for stream in streams:
            thread = threading.Thread(
                target=websocket_thread, args=(crypto.lower(), stream)
            )
            thread.daemon = True
            thread.start()

    # Keep the main thread alive
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
