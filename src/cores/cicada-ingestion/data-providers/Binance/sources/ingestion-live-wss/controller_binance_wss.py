import json
from datetime import datetime
import pandas as pd

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.sys.wss.server import WSSClient

# Initialize components with logging
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
        logger.log_info(f"WebSocket client initialized for {symbol} on stream {stream}",
                        context={'mod': 'BinanceWSSClient', 'action': 'Init'})

    def get_binance_wss_url(self, spot_market, testnet):
        url = ("wss://testnet.binance.vision/ws/" if testnet else "wss://stream.binance.com:9443/ws/") if spot_market \
            else ("wss://testnet.binancefuture.com/ws" if testnet else "wss://ws-fapi.binance.com/ws")
        logger.log_info("Determined Binance WebSocket URL", context={'mod': 'BinanceWSSClient', 'action': 'GetWSSUrl', 'url': url})
        return url

    def on_message(self, ws, message):
        """
        Handle incoming Binance messages based on the event type.
        """
        data = json.loads(message)
        event_type = data.get('e')
        logger.log_info(f"Message received for event type: {event_type}", context={'mod': 'BinanceWSSClient', 'action': 'OnMessage', 'event_type': event_type})

        try:
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
                logger.log_info(f"Unhandled event type: {event_type}", context={'mod': 'BinanceWSSClient', 'action': 'OnMessageUnhandled'})
        except Exception as e:
            logger.log_error(f"Error handling event {event_type}: {e}", context={'mod': 'BinanceWSSClient', 'action': 'HandleMessageError', 'event_type': event_type})

    def save_data_to_gcs(self, dataframe, stream_type):
        """
        Save data to GCS with path partitioning.
        """
        try:
            timestamp = dataframe['event_time'].iloc[0] if stream_type in ["kline", "trade", "aggTrade"] else datetime.now()
            year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute

            gcs_path = (f"Raw/WSS-binance/{self.spot_market}/{'testnet' if self.testnet else 'mainnet'}/"
                        f"{self.symbol}/{stream_type}/{year}/{month}/{day}/{hour}/{minute}/{stream_type}_data.parquet")

            if gcs_controller.check_file_exists(gcs_path):
                existing_df = gcs_controller.download_parquet_as_dataframe(gcs_path)
                combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
            else:
                combined_df = dataframe

            gcs_controller.upload_dataframe_to_gcs(combined_df, gcs_path, file_format='parquet')
            logger.log_info(f"{stream_type.capitalize()} data uploaded to GCS", context={'mod': 'BinanceWSSClient', 'action': 'SaveDataToGCS', 'gcs_path': gcs_path})

        except Exception as e:
            logger.log_error(f"Failed to upload {stream_type} data to GCS: {e}", context={'mod': 'BinanceWSSClient', 'action': 'SaveDataToGCSError', 'stream_type': stream_type})

    # Event-specific handlers with logging
    def handle_kline_update(self, data):
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
            'is_final': kline_data['x'],
            'ignore_field': kline_data['B']
        }])
        logger.log_info("Processed kline update", context={'mod': 'BinanceWSSClient', 'action': 'HandleKlineUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "kline")

    def handle_agg_trade_update(self, data):
        df = pd.DataFrame([{
            'aggregate_trade_id': data['a'],
            'price': float(data['p']),
            'quantity': float(data['q']),
            'first_trade_id': data['f'],
            'last_trade_id': data['l'],
            'trade_time': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        logger.log_info("Processed aggregate trade update", context={'mod': 'BinanceWSSClient', 'action': 'HandleAggTradeUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "aggTrade")

    def handle_trade_update(self, data):
        df = pd.DataFrame([{
            'trade_id': data['t'],
            'price': float(data['p']),
            'quantity': float(data['q']),
            'buyer_order_id': data['b'],
            'seller_order_id': data['a'],
            'timestamp': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        logger.log_info("Processed trade update", context={'mod': 'BinanceWSSClient', 'action': 'HandleTradeUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "trade")

    def handle_depth_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'update_id': data['u'],
            'bids': data['b'],
            'asks': data['a']
        }])
        logger.log_info("Processed depth update", context={'mod': 'BinanceWSSClient', 'action': 'HandleDepthUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "depthUpdate")

    def handle_mini_ticker_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'close_price': float(data['c']),
            'open_price': float(data['o']),
            'high_price': float(data['h']),
            'low_price': float(data['l']),
            'base_asset_volume': float(data['v']),
            'quote_asset_volume': float(data['q'])
        }])
        logger.log_info("Processed mini ticker update", context={'mod': 'BinanceWSSClient', 'action': 'HandleMiniTickerUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "miniTicker")

    def handle_ticker_update(self, data):
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
        logger.log_info("Processed ticker update", context={'mod': 'BinanceWSSClient', 'action': 'HandleTickerUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "ticker")

    def handle_book_ticker_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'best_bid_price': float(data['b']),
            'best_bid_qty': float(data['B']),
            'best_ask_price': float(data['a']),
            'best_ask_qty': float(data['A']),
        }])
        logger.log_info("Processed book ticker update", context={'mod': 'BinanceWSSClient', 'action': 'HandleBookTickerUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "bookTicker")

    def handle_rolling_window_ticker_update(self, data):
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
        logger.log_info("Processed rolling window ticker update", context={'mod': 'BinanceWSSClient', 'action': 'HandleRollingWindowTickerUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "rollingWindowTicker")

    def handle_avg_price_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'average_price': float(data['w']),
            'interval': data['i'],
            'trade_time': pd.to_datetime(data['T'], unit='ms')
        }])
        logger.log_info("Processed average price update", context={'mod': 'BinanceWSSClient', 'action': 'HandleAvgPriceUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "avgPrice")

    def handle_diff_depth_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'first_update_id': data['U'],
            'final_update_id': data['u'],
            'bids': data['b'],
            'asks': data['a']
        }])
        logger.log_info("Processed diff depth update", context={'mod': 'BinanceWSSClient', 'action': 'HandleDiffDepthUpdate', 'symbol': self.symbol})
        self.save_data_to_gcs(df, "depth")
