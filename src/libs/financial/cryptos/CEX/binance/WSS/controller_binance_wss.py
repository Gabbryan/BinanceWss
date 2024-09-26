from src.commons.env_manager.env_controller import EnvController
from src.libs.tools.sys.wss.server import WSSClient

env = EnvController("development")


class BinanceWSSClient(WSSClient):
    """
    A Binance-specific WebSocket client that extends the base WSSClient.
    This class handles Binance-specific WebSocket streams.
    """

    def __init__(self, symbol, stream="trade", spot_market=True, testnet=False):
        if spot_market:
            if not testnet:
                self.BINANCE_WSS_BASE_URL = "wss://stream.binance.com:9443/ws/"
            if testnet:
                self.BINANCE_WSS_BASE_URL = "wss:"

        if not spot_market:
            if not testnet:
                self.BINANCE_WSS_BASE_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
            if spot_market:
                self.BINANCE_WSS_BASE_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"

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
        print(f"Received {self.stream} data for {self.symbol}: {message}")

    def on_open(self, ws):
        """
        Called when the WebSocket connection is opened.
        """
        print(f"WebSocket connection opened for {self.symbol} on {self.stream} stream.")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Called when the WebSocket connection is closed.
        """
        print(f"WebSocket connection closed for {self.symbol}.")

    def on_error(self, ws, error):
        """
        Called when an error occurs during the WebSocket connection.
        """
        print(f"Error for {self.symbol} on {self.stream} stream: {error}")
