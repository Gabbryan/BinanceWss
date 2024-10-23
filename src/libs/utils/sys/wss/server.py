import ssl

import websocket


class WSSClient:
    """
    A base WebSocket client to handle WebSocket connections.
    """
    def __init__(self, url):
        self.url = url
        self.ws = None

    def connect(self):
        """
        Establish the WebSocket connection to the specified URL.
        """
        self.ws = websocket.WebSocketApp(self.url, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.ws.on_open = self.on_open
        print(self.ws.url)

        self.ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def on_message(self, ws, message):
        """
        Handle incoming messages.
        To be implemented in the subclass.
        """
        raise NotImplementedError("on_message method should be implemented by the subclass")

    def on_error(self, ws, error):
        """
        Handle errors during WebSocket communication.
        """
        print(f"Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """
        Handle WebSocket closure.
        """
        print("WebSocket connection closed")

    def on_open(self, ws):
        """
        Called when the WebSocket connection is opened.
        """
        print("WebSocket connection opened")
