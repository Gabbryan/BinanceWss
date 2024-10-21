import json

import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.sys.wss.server import WSSClient

env_controller = EnvController()
logger = LoggingController("BinanceWSSClient")
gcs_controller = GCSController(bucket_name=env_controller.get_yaml_config('wss-binance', 'bucket'))


class BinanceWSSClient(WSSClient):
    """
    A Binance-specific WebSocket client that extends the base WSSClient.
    This class handles Binance-specific WebSocket streams.
    """

    def __init__(self, symbol: str, stream="trade", spot_market=True, testnet=False):
        if spot_market:
            if not testnet:
                self.BINANCE_WSS_BASE_URL = "wss://stream.binance.com:9443/ws/"
            else:
                self.BINANCE_WSS_BASE_URL = "wss://testnet.binance.vision/ws/"
        else:
            if not testnet:
                self.BINANCE_WSS_BASE_URL = "wss://ws-fapi.binance.com/ws"
            else:
                self.BINANCE_WSS_BASE_URL = "wss://testnet.binancefuture.com/ws"

        url = f"{self.BINANCE_WSS_BASE_URL}{symbol.lower()}@{stream}"
        print(url)
        super().__init__(url)
        self.symbol = symbol
        self.stream = stream

    def on_message(self, ws, message):
        """
        Handle incoming Binance messages.
        This method is called when the WebSocket receives a message.
        """
        log_context = {
            'mod': 'WSSClient',
            'user': 'BinanceUser',
            'action': 'MessageReceived',
            'system': 'Binance'
        }
        logger.log_info(f"Received {self.stream} data for {self.symbol}: {message}", context=log_context)

        # Parse le message JSON
        data = json.loads(message)

        # Vérifier le type d'événement
        event_type = data.get('e')

        if event_type == 'depthUpdate':
            pass
        elif event_type == 'kline':
            self.handle_kline_update(data)

    def handle_kline_update(self, data):
        """
        Handle kline (candlestick) data and save or append it to a GCS Parquet file with partitioning down to the minute level.
        """
        # Extract kline data
        kline_data = data['k']

        # Create a DataFrame for the kline candlestick
        new_data_df = pd.DataFrame([{
            'timestamp': pd.to_datetime(kline_data['t'], unit='ms'),
            'symbol': kline_data['s'],
            'interval': kline_data['i'],
            'open': float(kline_data['o']),
            'close': float(kline_data['c']),
            'high': float(kline_data['h']),
            'low': float(kline_data['l']),
            'volume': float(kline_data['v']),
            'number_of_trades': int(kline_data['n']),
            'quote_asset_volume': float(kline_data['q']),
            'taker_buy_base_asset_volume': float(kline_data['V']),
            'taker_buy_quote_asset_volume': float(kline_data['Q']),
            'is_final': kline_data['x']  # True if the candle is closed
        }])

        # Extract the time information to build the partitioned path
        interval = kline_data['i']
        timestamp = pd.to_datetime(kline_data['t'], unit='ms')
        year = timestamp.year
        month = timestamp.month
        day = timestamp.day
        hour = timestamp.hour
        minute = timestamp.minute

        # Define the GCS path with partitions down to the minute level
        gcs_path = (f"Raw/WSS-binance/{self.symbol}/kline/{interval}/{year}/{month}/{day}/"
                    f"{hour}/{minute}/kline_data.parquet")

        try:
            # Check if the Parquet file already exists at the minute level
            if gcs_controller.check_file_exists(gcs_path):
                # Download the existing Parquet file from GCS
                existing_df = gcs_controller.download_parquet_as_dataframe(gcs_path)
                # Concatenate the new data with the existing data
                combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
            else:
                combined_df = new_data_df

            # Upload the combined data back to GCS
            gcs_controller.upload_dataframe_to_gcs(combined_df, gcs_path, file_format='parquet', is_exist_verification=False)
            logger.log_info(f"Kline data uploaded to GCS: {gcs_path}", context={'mod': 'WSSClient', 'action': 'KlineDataUploaded'})

        except Exception as e:
            logger.log_error(f"Failed to upload kline data to GCS: {e}", context={'mod': 'WSSClient', 'action': 'KlineDataUploadError'})
