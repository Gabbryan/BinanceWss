import json
from datetime import datetime
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
    A Binance-specific WebSocket client that handles specific WebSocket streams.
    """

    def __init__(self, symbol: str, stream="trade", spot_market=True, testnet=False):
        self.spot_market = spot_market
        self.testnet = testnet
        self.BINANCE_WSS_BASE_URL = self.get_binance_wss_url(spot_market, testnet)

        url = f"{self.BINANCE_WSS_BASE_URL}{symbol.lower()}@{stream}"
        super().__init__(url)
        self.symbol = symbol
        self.stream = stream
        logger.log_info(f"WebSocket client initialized for {symbol} on stream {stream}")

    def get_binance_wss_url(self, spot_market, testnet):
        if spot_market:
            return "wss://testnet.binance.vision/ws/" if testnet else "wss://stream.binance.com:9443/ws/"
        else:
            return "wss://testnet.binancefuture.com/ws" if testnet else "wss://ws-fapi.binance.com/ws"

    def on_message(self, ws, message):
        """
        Handle incoming Binance messages based on the event type.
        """
        data = json.loads(message)
        event_type = data.get('e')

        handler_mapping = {
            'kline': self.handle_kline_update,
            'aggTrade': self.handle_agg_trade_update,
            'trade': self.handle_trade_update,
            'depthUpdate': self.handle_depth_update,
            '24hrMiniTicker': self.handle_mini_ticker_update,
            '24hrTicker': self.handle_ticker_update,
            'bookTicker': self.handle_book_ticker_update,
            'rollingWindowTicker': self.handle_rolling_window_ticker_update,
            'avgPrice': self.handle_avg_price_update,
            'depth': self.handle_diff_depth_update
        }

        handler = handler_mapping.get(event_type)
        if handler:
            handler(data)
            logger.log_info(f"Processed {event_type} event for {self.symbol}")
        else:
            logger.log_info(f"Unhandled event type: {event_type}")

    def handle_kline_update(self, data):
        """
        Handle kline (candlestick) data and store it.
        """
        kline_data = data['k']
        df = pd.DataFrame([{
            'event_time': pd.to_datetime(data['E'], unit='ms'),
            'symbol': kline_data['s'],
            'interval': kline_data['i'],
            'start_time': pd.to_datetime(kline_data['t'], unit='ms'),
            'close_time': pd.to_datetime(kline_data['T'], unit='ms'),
            'first_trade_id': int(kline_data['f']),
            'last_trade_id': int(kline_data['L']),
            'open': float(kline_data['o']),
            'close': float(kline_data['c']),
            'high': float(kline_data['h']),
            'low': float(kline_data['l']),
            'volume': float(kline_data['v']),
            'number_of_trades': int(kline_data['n']),
            'quote_asset_volume': float(kline_data['q']),
            'taker_buy_base_asset_volume': float(kline_data['V']),
            'taker_buy_quote_asset_volume': float(kline_data['Q']),
            'is_final': kline_data['x'],  # True if the candle is closed
            'ignore_field': kline_data['B']  # Ignored field for reference
        }])

        self.save_data_to_gcs(df, "kline")

    def handle_agg_trade_update(self, data):
        """
        Handle aggregate trade stream data and store it.
        """
        df = pd.DataFrame([{
            'aggregate_trade_id': data['a'],
            'price': float(data['p']),
            'quantity': float(data['q']),
            'first_trade_id': data['f'],
            'last_trade_id': data['l'],
            'trade_time': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        self.save_data_to_gcs(df, "aggTrade")

    # Other handlers (handle_trade_update, handle_depth_update, etc.) omitted for brevity but follow the same pattern

    def save_data_to_gcs(self, dataframe, stream_type):
        """
        Save data to GCS with path partitioning.
        """
        # Extract the first timestamp from the 'start_time' column
        timestamp = dataframe['event_time'].iloc[0] if 'event_time' in dataframe.columns else datetime.now()
        year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute

        gcs_path = (f"Raw/WSS-binance/{self.spot_market}/"
                    f"{'testnet' if self.testnet else 'mainnet'}/"
                    f"{self.symbol}/{stream_type}/{year}/{month}/{day}/"
                    f"{hour}/{minute}/{stream_type}_data.parquet")

        try:
            if gcs_controller.check_file_exists(gcs_path):
                existing_df = gcs_controller.download_parquet_as_dataframe(gcs_path)
                combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
            else:
                combined_df = dataframe

            gcs_controller.upload_dataframe_to_gcs(combined_df, gcs_path, file_format='parquet')
            logger.log_info(f"{stream_type.capitalize()} data uploaded to GCS: {gcs_path}")

        except Exception as e:
            logger.log_error(f"Failed to upload {stream_type} data to GCS: {e}")
