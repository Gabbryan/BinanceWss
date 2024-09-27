from src.commons.env_manager.env_controller import EnvController
from src.libs.third_services.google.pub_sub.server_pub_sub import ServerPubSub

env = EnvController()


class ControllerPubSub:
    """
    A controller class for handling WebSocket return messages.
    """

    def __init__(self, message, topic_id, parquet_path=None, pub_sub=False):
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
        self.pub_sub_controller = ServerPubSub(project_id=env.get_env("GOOGLE_CLOUD_PROJECT"), topic_id=topic_id)

    def parse_message(self):
        """
        Parse the WebSocket message and extract relevant data.
        """
        return self.message

    def publish_message(self):
        """
        Publish the parsed message to a message broker.
        """
        # TODO: Implement message publishing logic
        response = {"status": "ERROR", "error": "Not implemented"}
        if self.pub_sub:
            if not self.message is None:
                self.data = self.parse_message()
                response = self.pub_sub_controller.publish_message(self.data)
        if self.parquet_path is not None:
            self.save_message()
        return response

    def save_message(self, format="parquet"):
        """
        Save the parsed message to a file.
        :param format: The format to save the data (default is 'parquet').
        """
        return 1
