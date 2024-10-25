import json
from datetime import datetime
import pandas as pd
from pathlib import Path

from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.google_cloud_bucket.controller_gcs import GCSController
from src.libs.utils.sys.wss.server import WSSClient
from src.libs.utils.dataframe.validation.df_verification_controller import DataFrameVerificationController

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

        # Load configurations from JSON file
        self.configurations = self.load_configurations()

    def get_binance_wss_url(self, spot_market, testnet):
        url = ("wss://testnet.binance.vision/ws/" if testnet else "wss://stream.binance.com:9443/ws/") if spot_market \
            else ("wss://testnet.binancefuture.com/ws" if testnet else "wss://ws-fapi.binance.com/ws")
        logger.log_info("Determined Binance WebSocket URL",
                        context={'mod': 'BinanceWSSClient', 'action': 'GetWSSUrl', 'url': url})
        return url

    def load_configurations(self):
        """
        Load configurations from the JSON file.
        """
        config_path = Path(__file__).parent / 'binance_stream_config.json'
        try:
            with open(config_path, 'r') as file:
                configurations = json.load(file)
            logger.log_info("Loaded configurations from JSON file",
                            context={'mod': 'BinanceWSSClient', 'action': 'LoadConfigurations'})
            return configurations
        except Exception as e:
            logger.log_error(f"Failed to load configurations: {e}",
                             context={'mod': 'BinanceWSSClient', 'action': 'LoadConfigurationsError'})
            return {}

    def on_message(self, ws, message):
        """
        Handle incoming Binance messages based on the event type.
        """
        data = json.loads(message)
        event_type = data.get('e')
        logger.log_info(f"Message received for event type: {event_type}",
                        context={'mod': 'BinanceWSSClient', 'action': 'OnMessage', 'event_type': event_type})

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
            elif event_type == 'avgPrice':
                self.handle_avg_price_update(data)
            elif event_type == 'depth':
                self.handle_diff_depth_update(data)
            else:
                logger.log_info(f"Unhandled event type: {event_type}",
                                context={'mod': 'BinanceWSSClient', 'action': 'OnMessageUnhandled'})
        except Exception as e:
            logger.log_error(f"Error handling event {event_type}: {e}",
                             context={'mod': 'BinanceWSSClient', 'action': 'HandleMessageError', 'event_type': event_type})

    def save_data_to_gcs(self, dataframe, stream_type):
        """
        Save data to GCS with path partitioning.
        """
        try:
            timestamp_col = 'event_time' if 'event_time' in dataframe.columns else \
                'trade_time' if 'trade_time' in dataframe.columns else \
                    'timestamp' if 'timestamp' in dataframe.columns else datetime.now()

            timestamp = dataframe[timestamp_col].iloc[0] if isinstance(timestamp_col, str) else timestamp_col
            year, month, day, hour, minute = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute

            gcs_path = (f"Raw/WSS-binance/{'spot' if self.spot_market else 'futures'}/{'testnet' if self.testnet else 'mainnet'}/"
                        f"{self.symbol}/{stream_type}/{year}/{month:02d}/{day:02d}/{hour:02d}/{minute:02d}/{stream_type}_data.parquet")

            if gcs_controller.check_file_exists(gcs_path):
                existing_df = gcs_controller.download_parquet_as_dataframe(gcs_path)
                combined_df = pd.concat([existing_df, dataframe], ignore_index=True)
            else:
                combined_df = dataframe

            gcs_controller.upload_dataframe_to_gcs(combined_df, gcs_path, file_format='parquet')
            logger.log_info(f"{stream_type.capitalize()} data uploaded to GCS",
                            context={'mod': 'BinanceWSSClient', 'action': 'SaveDataToGCS', 'gcs_path': gcs_path})

        except Exception as e:
            logger.log_error(f"Failed to upload {stream_type} data to GCS: {e}",
                             context={'mod': 'BinanceWSSClient', 'action': 'SaveDataToGCSError', 'stream_type': stream_type})

    def validate_dataframe(self, df, stream_type):
        """
        Validates the DataFrame using DataFrameVerificationController.
        """
        config = self.configurations.get(stream_type)
        if config:
            required_columns = config.get('required_columns', [])
            data_type_checks = config.get('data_type_checks', {})
            verification_controller = DataFrameVerificationController(
                required_columns=required_columns,
                data_type_checks=data_type_checks
            )
            df = verification_controller.validate_and_transform_dataframe(
                df, clean_data=True, context={'stream_type': stream_type})
            return df
        else:
            logger.log_warning(f"No configuration found for stream type: {stream_type}",
                               context={'mod': 'BinanceWSSClient', 'action': 'ValidateDataFrame', 'stream_type': stream_type})
            return df

    # Event-specific handlers with validation
    def handle_kline_update(self, data):
        kline_data = data['k']
        df = pd.DataFrame([{
            'event_date': pd.to_datetime(data['E'], unit='ms'),
            'symbol': kline_data['s'],
            'interval': kline_data['i'],
            'start_date': pd.to_datetime(kline_data['t'], unit='ms'),
            'close_date': pd.to_datetime(kline_data['T'], unit='ms'),
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
            'is_final': kline_data['x']
        }])
        logger.log_info("Processed kline update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleKlineUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'kline')

        self.save_data_to_gcs(df, "kline")

    def handle_agg_trade_update(self, data):
        df = pd.DataFrame([{
            'aggregate_trade_id': int(data['a']),
            'price': float(data['p']),
            'quantity': float(data['q']),
            'first_trade_id': int(data['f']),
            'last_trade_id': int(data['l']),
            'trade_date': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        logger.log_info("Processed aggregate trade update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleAggTradeUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'aggTrade')

        self.save_data_to_gcs(df, "aggTrade")

    def handle_trade_update(self, data):
        df = pd.DataFrame([{
            'trade_id': int(data['t']),
            'price': float(data['p']),
            'quantity': float(data['q']),
            'buyer_order_id': int(data['b']),
            'seller_order_id': int(data['a']),
            'timestamp': pd.to_datetime(data['T'], unit='ms'),
            'is_market_maker': data['m']
        }])
        logger.log_info("Processed trade update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleTradeUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'trade')

        self.save_data_to_gcs(df, "trade")

    def handle_depth_update(self, data):
        """
        Handle depth update messages from the Binance WebSocket.
        Converts bids and asks from lists to dictionaries for easier processing.
        """
        df = pd.DataFrame([{
            'symbol': data['s'],
            'update_id': data['u'],
            'bids': {price: qty for price, qty in zip(data['b'][0], data['b'][1])},  # Convert bids to dictionary
            'asks': {price: qty for price, qty in zip(data['a'][0], data['a'][1])}   # Convert asks to dictionary
        }])

        logger.log_info("Processed depth update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleDepthUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'depthUpdate')

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
        logger.log_info("Processed mini ticker update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleMiniTickerUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, '24hrMiniTicker')

        self.save_data_to_gcs(df, "24hrMiniTicker")

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
        logger.log_info("Processed ticker update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleTickerUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, '24hrTicker')

        self.save_data_to_gcs(df, "24hrTicker")

    def handle_book_ticker_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'best_bid_price': float(data['b']),
            'best_bid_qty': float(data['B']),
            'best_ask_price': float(data['a']),
            'best_ask_qty': float(data['A'])
        }])
        logger.log_info("Processed book ticker update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleBookTickerUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'bookTicker')

        self.save_data_to_gcs(df, "bookTicker")

    def handle_avg_price_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'average_price': float(data['w']),
            'interval': data['i'],
            'trade_time': pd.to_datetime(data['T'], unit='ms')
        }])
        logger.log_info("Processed average price update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleAvgPriceUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'avgPrice')

        self.save_data_to_gcs(df, "avgPrice")

    def handle_diff_depth_update(self, data):
        df = pd.DataFrame([{
            'symbol': data['s'],
            'first_update_id': data['U'],
            'final_update_id': data['u'],
            'bids': data['b'],
            'asks': data['a']
        }])
        logger.log_info("Processed diff depth update",
                        context={'mod': 'BinanceWSSClient', 'action': 'HandleDiffDepthUpdate', 'symbol': self.symbol})

        # Validate DataFrame
        df = self.validate_dataframe(df, 'depth')

        self.save_data_to_gcs(df, "depth")

