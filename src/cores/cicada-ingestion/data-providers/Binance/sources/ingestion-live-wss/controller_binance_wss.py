from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController
from src.libs.third_services.google.pub_sub.controller_pub_sub import ControllerPubSub
from src.libs.tools.sys.wss.server import WSSClient

env_controller = EnvController("development")
logger = LoggingController()


class BinanceWSSClient(WSSClient):
    """
    A Binance-specific WebSocket client that extends the base WSSClient.
    This class handles Binance-specific WebSocket streams.
    """

    def __init__(self, symbol, stream="trade", spot_market=True, testnet=False):
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

        """
        Initialize the Binance WebSocket client with a trading symbol and stream type.
        :param symbol: The trading symbol (e.g., 'btcusdt').
        :param stream: The stream type (e.g., 'trade', 'depth', etc.).
        """
        url = f"{self.BINANCE_WSS_BASE_URL}{symbol}@{stream}"
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
        pub_sub_controller = ControllerPubSub(message, pub_sub=True, topic_id=env_controller.get_env("PUB_SUB_TOPIC_ID"))
        response = pub_sub_controller.publish_message()
        if response['status'] == 'ERROR':
            logger.log_error(f"Error while publishing message: {response['error']}", context=log_context)
        else:
            logger.log_info(f"Message published successfully: {response['status']}", context=log_context)
        pass

    def on_open(self, ws):
        """
        Called when the WebSocket connection is opened.
        """
        log_context = {
            'mod': 'WSSClient',
            'user': 'BinanceUser',
            'action': 'ConnectionOpened',
            'system': 'Binance'
        }
        logger.log_info(f"WebSocket connection opened for {self.symbol} on {self.stream} stream.", context=log_context)

    def on_close(self, ws, close_status_code, close_msg):
        """
        Called when the WebSocket connection is closed.
        """
        log_context = {
            'mod': 'WSSClient',
            'user': 'BinanceUser',
            'action': 'ConnectionClosed',
            'system': 'Binance'
        }
        logger.log_warning(f"WebSocket connection closed for {self.symbol}. Code: {close_status_code}, Message: {close_msg}", context=log_context)

    def on_error(self, ws, error):
        """
        Called when an error occurs during the WebSocket connection.
        """
        log_context = {
            'mod': 'WSSClient',
            'user': 'BinanceUser',
            'action': 'ErrorOccurred',
            'system': 'Binance'
        }
        logger.log_error(f"Error for {self.symbol} on {self.stream} stream: {error}", context=log_context)
