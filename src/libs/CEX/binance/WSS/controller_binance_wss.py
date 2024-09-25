from src.libs.tools.return_message.WSS.controller_wss_return_message import ControllerWSSReturnMessage
from src.libs.wss.server import WSSClient


class BinanceWSSClient(WSSClient):
    """
    A Binance-specific WebSocket client that extends the base WSSClient.
    This class handles Binance-specific WebSocket streams.
    """

    def __init__(self, symbol, stream="trade", wss_base_url="wss://stream.binance.com:9443/ws/"):
        """
        Initialize the Binance WebSocket client with a trading symbol and stream type.
        :param symbol: The trading symbol (e.g., 'btcusdt').
        :param stream: The stream type (e.g., 'trade', 'depth', etc.).
        """
        self.WSS_base_url = wss_base_url
        url = f"{self.WSS_base_url}{symbol}@{stream}"
        super().__init__(url)
        self.symbol = symbol
        self.stream = stream

    def on_message(self, ws, message):
        """
        Handle incoming Binance messages.
        This method is called when the WebSocket receives a message.

        """
        wss_response = ControllerWSSReturnMessage(message, self.symbol, self.stream)
        wss_response.publish_message()

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
