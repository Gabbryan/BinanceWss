from src.libs.google.pub_sub.controller_pub_sub import PubSubController


class ControllerWSSReturnMessage:
    """
    A controller class for handling WebSocket return messages.
    """

    def __init__(self, message, parquet_path, pub_sub=False):
        """
        Initialize the controller with a WebSocket message and a path for saving the data.
        :param message: The WebSocket message.
        :param parquet_path: The path for saving the data.
        :param pub_sub: Whether the message is from a pub/sub channel (default is False).
        """
        self.message = message
        self.parquet_path = parquet_path
        self.pub_sub = pub_sub
        self.data = None
        self.error = None
        self.status = None

    def parse_message(self):
        """
        Parse the WebSocket message and extract relevant data.
        """
        # TODO: Implement message parsing logic

        return {}

    def publish_message(self):
        """
        Publish the parsed message to a message broker.
        """
        # TODO: Implement message publishing logic
        if self.pub_sub:
            if not self.data is None:
                self.data = self.parse_message()
                if self.data["e"] == "trade":
                    pub_sub_controller = PubSubController(project_id="project_id", topic_id="topic_id")
                    response = pub_sub_controller.publish_message(self.parquet_path, self.data)
        else:
            response = {"status": "ERROR", "error": "Not implemented"}
        return response

    def save_message(self, format="parquet"):
        """
        Save the parsed message to a file.
        :param format: The format to save the data (default is 'parquet').
        """
        # TODO: Implement message saving logic
        return []
