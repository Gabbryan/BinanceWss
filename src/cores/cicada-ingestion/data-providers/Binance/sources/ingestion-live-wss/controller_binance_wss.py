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
        # Set URLs for spot or futures market and testnet or mainnet
        self.spot_market = spot_market
        self.testnet = testnet
        self.BINANCE_WSS_BASE_URL = self.get_binance_wss_url(spot_market, testnet)

        url = f"{self.BINANCE_WSS_BASE_URL}{symbol.lower()}@{stream}"
        super().__init__(url)
        self.symbol = symbol
        self.stream = stream

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

        if event_type == 'kline':
            self.handle_kline_update(data)
        elif event_type == 'aggTrade':
            self.handle_agg_trade_update(data)
        elif event_type == 'trade':
            self.handle_trade_update(data)
        elif event_type == 'depthUpdate':
            self.handle_depth_update(data)
        elif event_type == '24hrMiniTicker':
            self.handle_mini_ticker_update(data)
        elif event_type == '24hrTicker':
            self.handle_ticker_update(data)
        elif event_type == 'bookTicker':
            self.handle_book_ticker_update(data)
        elif event_type == 'rollingWindowTicker':
            self.handle_rolling_window_ticker_update(data)
        elif event_type == 'avgPrice':
            self.handle_avg_price_update(data)
        elif event_type == 'depth':
            self.handle_diff_depth_update(data)
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

        # Save the data to GCS with appropriate partitioning
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

    def handle_trade_update(self, data):
        """
        Handle trade stream data and store it.
        """
        df = pd.DataFrame([{
            'trade_id': data['t'],
            'price': float(data['p']),
            'quantity': float(data['q']),
            'buyer_order_id': data['b'],
            'seller_order_id': data['a'],
            'timestamp': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        self.save_data_to_gcs(df, "trade")

    def handle_depth_update(self, data):
        """
        Handle depth update (order book) stream data and store it.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'update_id': data['u'],
            'bids': data['b'],  # List of bid prices and volumes
            'asks': data['a'],  # List of ask prices and volumes
        }])
        self.save_data_to_gcs(df, "depthUpdate")

    def handle_mini_ticker_update(self, data):
        """
        Handle 24hr rolling window mini-ticker statistics.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'close_price': float(data['c']),
            'open_price': float(data['o']),
            'high_price': float(data['h']),
            'low_price': float(data['l']),
            'base_asset_volume': float(data['v']),
            'quote_asset_volume': float(data['q'])
        }])
        self.save_data_to_gcs(df, "miniTicker")

    def handle_ticker_update(self, data):
        """
        Handle 24hr rolling window ticker statistics.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'price_change': float(data['p']),
            'price_change_percent': float(data['P']),
            'weighted_avg_price': float(data['w']),
            'last_price': float(data['c']),
            'open_price': float(data['o']),
            'high_price': float(data['h']),
            'low_price': float(data['l']),
            'base_asset_volume': float(data['v']),
            'quote_asset_volume': float(data['q'])
        }])
        self.save_data_to_gcs(df, "ticker")

    def handle_book_ticker_update(self, data):
        """
        Handle book ticker (top bid/ask prices) stream data.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'best_bid_price': float(data['b']),
            'best_bid_qty': float(data['B']),
            'best_ask_price': float(data['a']),
            'best_ask_qty': float(data['A']),
        }])
        self.save_data_to_gcs(df, "bookTicker")

    def handle_rolling_window_ticker_update(self, data):
        """
        Handle individual symbol rolling window statistics.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'price_change': float(data['p']),
            'price_change_percent': float(data['P']),
            'open_price': float(data['o']),
            'high_price': float(data['h']),
            'low_price': float(data['l']),
            'last_price': float(data['c']),
            'volume': float(data['v']),
            'quote_volume': float(data['q']),
            'first_trade_id': int(data['F']),
            'last_trade_id': int(data['L']),
            'number_of_trades': int(data['n'])
        }])
        self.save_data_to_gcs(df, "rollingWindowTicker")

    def handle_avg_price_update(self, data):
        """
        Handle average price stream data.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'average_price': float(data['w']),
            'interval': data['i'],
            'trade_time': pd.to_datetime(data['T'], unit='ms')
        }])
        self.save_data_to_gcs(df, "avgPrice")

    def handle_diff_depth_update(self, data):
        """
        Handle diff. depth stream data and store it.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'first_update_id': data['U'],
            'final_update_id': data['u'],
            'bids': data['b'],  # List of bid prices and volumes
            'asks': data['a']  # List of ask prices and volumes
        }])
        self.save_data_to_gcs(df, "depth")

    def save_data_to_gcs(self, dataframe, stream_type):
        """
        Save data to GCS with path partitioning.
        """
        # Extract the first timestamp from the 'start_time' column
        if stream_type == "kline" or stream_type == "trade" or stream_type == "aggTrade":
            timestamp = dataframe['event_time'].iloc[0]  # Extract the first timestamp (assumed to be consistent)
        elif stream_type == "aggTrade":
            timestamp = dataframe['trade_time'].iloc[0]
        else:
            timestamp = datetime.now()

        year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute

        # Define GCS path based on the stream type and timestamp
        gcs_path = (f"Raw/WSS-binance/{self.spot_market}/"
                    f"{'testnet' if self.testnet else 'mainnet'}/"
                    f"{self.symbol}/{stream_type}/{year}/{month}/{day}/"
                    f"{hour}/{minute}/{stream_type}_data.parquet")

        try:
            # Check if the file exists and append data if necessary
            if gcs_controller.check_file_exists(gcs_path):
                existing_df = gcs_controller.download_parquet_as_dataframe(gcs_path)
                combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
            else:
                combined_df = dataframe

            # Upload the combined dataframe to GCS
            gcs_controller.upload_dataframe_to_gcs(combined_df, gcs_path, file_format='parquet')
            logger.log_info(f"{stream_type.capitalize()} data uploaded to GCS: {gcs_path}")

        except Exception as e:
            logger.log_error(f"Failed to upload {stream_type} data to GCS: {e}")
