import asyncio
import configparser
import json
import logging
import os
from collections import defaultdict
from datetime import datetime

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import websocket
from dotenv import load_dotenv

from slack_package import init_slack, get_slack_decorators

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)


class Config:
    def __init__(self):
        load_dotenv()
        self.CONFIG_PATH = "./config.conf"
        self.LOG_DIR = "logs"
        self.BEARER = os.getenv("BEARER_TOKEN")
        self.EXCHANGE = os.getenv("EXCHANGE", "binance").lower()
        self.WEBSOCKET_URLS = {"binance": "wss://fstream.binance.com/ws"}
        self.WEBSOCKET_URL = self.WEBSOCKET_URLS.get(
            self.EXCHANGE, self.WEBSOCKET_URLS["binance"]
        )
        self.WEBHOOK_URL = os.getenv("WEBHOOK_URL")
        self.AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
        self.AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
        self.AWS_REGION_NAME = os.getenv("AWS_REGION_NAME")
        self.S3_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
        self.ANALYSIS_SERVICE_URL = os.getenv(
            "ANALYSIS_SERVICE_URL", "http://analysis-agg-trades:8000"
        )

        self._validate_config()
        self._load_config()

    def _validate_config(self):
        if (
            not self.WEBHOOK_URL
            or self.WEBHOOK_URL == "https://hooks.slack.com/services/your/webhook/url"
        ):
            raise ValueError(
                "WEBHOOK_URL is not defined or invalid. Please set it in your .env file."
            )

    def _load_config(self):
        config = configparser.ConfigParser()
        config.read(self.CONFIG_PATH)
        self.stream = config["settings"]["stream"]
        self.cryptos = config["settings"]["cryptos"].split(",")


config = Config()
request_times = defaultdict(list)
total_requests = 0
slack_channel = init_slack(config.WEBHOOK_URL)
slack_decorators = get_slack_decorators()
last_timestamps = {}


async def notify_analysis_service(file_path):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{config.ANALYSIS_SERVICE_URL}/analyze", json={"file_path": file_path}
            )
            response.raise_for_status()
            result = response.json()
            logging.info(
                f"File {file_path} {'passed' if result['status'] == 'success' else 'failed'} analysis"
            )
        except Exception as e:
            logging.error(f"Error calling analysis service: {str(e)}")


def save_dataframe_to_s3(df, exchange, symbol, day, hour):
    base_path = f"s3://{config.S3_BUCKET_NAME}/cicada-data/aggTrades/live/ingestion/{exchange}/{symbol}/{day}/{hour}/"
    fs = s3fs.S3FileSystem(
        key=config.AWS_ACCESS_KEY_ID, secret=config.AWS_SECRET_ACCESS_KEY
    )

    df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
    grouped = df.groupby(df["datetime"].dt.strftime("%M"))

    for minute, group in grouped:
        file_path = f"{base_path}{minute}/data.parquet"

        if fs.exists(file_path):
            existing_table = pq.read_table(file_path, filesystem=fs)
            existing_df = existing_table.to_pandas()
            combined_df = pd.concat([existing_df, group], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=["timestamp"], keep="last"
            ).sort_values("timestamp")
        else:
            combined_df = group

        table = pa.Table.from_pandas(combined_df)
        pq.write_table(table, file_path, filesystem=fs)
        logging.info(f"DataFrame uploaded to S3 path: {file_path}")


def process_market_data(message_data, symbol):
    global total_requests, request_times, last_timestamps

    if message_data.get("e") != "aggTrade":
        logging.info(f"Ignoring non-aggTrade message: {message_data}")
        return

    event_time = datetime.utcfromtimestamp(message_data["E"] / 1000)
    readable_time = event_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    last_timestamps[symbol] = event_time

    logging.info(f"Processing message for {symbol} at {readable_time} UTC")

    try:
        df = pd.DataFrame(
            {
                "market_raw_id": [1],
                "price": [float(message_data["p"])],
                "quantity": [float(message_data["q"])],
                "timestamp": [float(message_data["E"])],
                "is_buyer_market": [bool(message_data["m"])],
                "is_best_match": [False],
                "day": [event_time.strftime("%Y-%m-%d")],
                "hour": [event_time.strftime("%H")],
                "minute": [event_time.strftime("%M")],
            }
        )
        save_dataframe_to_s3(df, config.EXCHANGE, symbol, df["day"][0], df["hour"][0])
        total_requests += 1
        request_times[df["day"][0]].append(event_time.timestamp())
        save_request_times(df["day"][0])
    except Exception as e:
        logging.error(
            f"Error processing market data: {e} with message_data: {message_data}"
        )


def on_message(ws, message, symbol):
    message_data = json.loads(message)
    process_market_data(message_data, symbol)


def on_error(ws, error):
    logging.error(f"WebSocket error: {error}")
    ws.close()


def on_close(ws, close_status_code, close_msg):
    logging.error(f"WebSocket closed: {close_status_code} - {close_msg}")


def websocket_thread(symbol):
    while True:
        ws = websocket.WebSocketApp(
            f"{config.WEBSOCKET_URL}/{symbol}@{config.stream}",
            on_message=lambda ws, msg: on_message(ws, msg, symbol),
            on_error=on_error,
            on_close=on_close,
        )
        logging.info(f"WebSocket connection established for {symbol}")
        ws.run_forever(ping_interval=60, ping_timeout=10)
        logging.error(f"WebSocket connection closed for {symbol}. Reconnecting...")


def save_request_times(day):
    file_path = os.path.join(config.LOG_DIR, f"request_times_{day}.json")
    with open(file_path, "w") as file:
        json.dump(request_times[day], file)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{config.LOG_DIR}/log_ws.log"),
        ],
    )


async def periodic_log():
    while True:
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(f"Service is running. Current time: {now}")
        await asyncio.sleep(60)


def analyze_symbol(symbol):
    if symbol not in last_timestamps:
        logging.warning(f"No timestamp available for {symbol}. Skipping analysis.")
        return

    last_time = last_timestamps[symbol]
    readable_time = last_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    day_str = last_time.strftime("%Y-%m-%d")
    hour_str = last_time.strftime("%H")
    minute_str = last_time.strftime("%M")

    base_path = f"s3://{config.S3_BUCKET_NAME}/cicada-data/aggTrades/live/ingestion/{config.EXCHANGE}/{symbol}/{day_str}/{hour_str}/"
    file_path = f"{base_path}{minute_str}/data.parquet"

    logging.info(f"Triggering analysis for {symbol} at {readable_time} UTC")
    asyncio.run(notify_analysis_service(file_path))


async def analysis_scheduler():
    while True:
        await asyncio.sleep(60)  # Run every minute
        for symbol in config.cryptos:
            analyze_symbol(symbol.lower())


async def main():
    global last_timestamps
    last_timestamps = {crypto.lower(): datetime.utcnow() for crypto in config.cryptos}

    os.makedirs(config.LOG_DIR, exist_ok=True)
    setup_logging()
    logging.info("Service is starting...")

    tasks = [
        asyncio.create_task(periodic_log()),
        asyncio.create_task(analysis_scheduler()),
    ]

    for crypto in config.cryptos:
        crypto = crypto.lower()
        tasks.append(asyncio.to_thread(websocket_thread, crypto))

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logging.info("Tasks are being cancelled")
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info("All tasks have been cancelled")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Received keyboard interrupt, shutting down...")
    finally:
        logging.info("Service has been shut down")
