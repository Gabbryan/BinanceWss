import ssl
import websocket
from src.commons.logs.logging_controller import LoggingController


class WSSClient:
    """
    A base WebSocket client to handle WebSocket connections.
    """
    def __init__(self, url):
        self.url = url
        self.ws = None
        self.logger = LoggingController("WSSClient")
        self.logger.log_info("WSSClient initialized with URL.", context={'mod': 'WSSClient', 'action': 'Init'})

    def connect(self):
        """
        Establish the WebSocket connection to the specified URL.
        """
        try:
            self.ws = websocket.WebSocketApp(
                self.url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            self.ws.on_open = self.on_open
            self.logger.log_info("Connecting to WebSocket.", context={'mod': 'WSSClient', 'action': 'Connect'})
            self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
        except Exception as e:
            self.logger.log_error(f"Error establishing WebSocket connection: {e}", context={'mod': 'WSSClient', 'action': 'ConnectError'})

    def on_message(self, ws, message):
        """
        Handle incoming messages.
        To be implemented in the subclass.
        """
        self.logger.log_info(f"Message received: {message}", context={'mod': 'WSSClient', 'action': 'OnMessage'})
        raise NotImplementedError("on_message method should be implemented by the subclass")

    def on_error(self, ws, error):
        """
        Handle errors during WebSocket communication.
        """
        self.logger.log_error(f"WebSocket error: {error}", context={'mod': 'WSSClient', 'action': 'OnError'})

    def on_close(self, ws, close_status_code, close_msg):
        """
        Handle WebSocket closure.
        """
        self.logger.log_info(f"WebSocket connection closed. Status code: {close_status_code}, message: {close_msg}", context={'mod': 'WSSClient', 'action': 'OnClose'})

    def on_open(self, ws):
        """
        Called when the WebSocket connection is opened.
        """
        self.logger.log_info("WebSocket connection opened", context={'mod': 'WSSClient', 'action': 'OnOpen'})
