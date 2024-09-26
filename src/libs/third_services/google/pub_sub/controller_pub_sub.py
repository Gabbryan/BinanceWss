import json

from google.cloud import pubsub_v1


class PubSubController:
    """
    A controller to manage Google Cloud Pub/Sub publishing.
    Can be extended or integrated with other clients or controllers.
    """

    def __init__(self, project_id, topic_id):
        """
        Initialize the Pub/Sub client and topic path.
        :param project_id: Google Cloud Project ID.
        :param topic_id: Pub/Sub Topic ID.
        """
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)

    async def publish_message(self, symbol, message):
        """
        Asynchronously publish a message to Pub/Sub.
        :param symbol: The symbol to associate with the message.
        :param message: The message to be published.
        """
        try:
            # Prepare the message as bytes
            message_data = json.dumps(message).encode("utf-8")
            # Publish the message to the specified Pub/Sub topic
            future = self.publisher.publish(
                self.topic_path, data=message_data, symbol=symbol
            )
            # Wait for the result of the publish (blocking or awaiting the result)
            future.result()  # Will raise an exception if publish fails
        except Exception as e:
            # Handle error
            print(f"Error publishing message for {symbol}: {str(e)}")
