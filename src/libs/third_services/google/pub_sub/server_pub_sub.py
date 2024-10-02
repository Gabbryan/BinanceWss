import json

from google.cloud import pubsub_v1

from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("pub_sub")


class ServerPubSub:
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
        logger.log_info(f"Initialized PubSubController for project {project_id} and topic {topic_id}", context={'mod': 'PubSubController', 'action': 'Init'})

    async def publish_message(self, message):
        """
        Asynchronously publish a message to Pub/Sub.
        :param symbol: The symbol to associate with the message.
        :param message: The message to be published.
        """
        try:
            logger.log_info(f"Publishing message", context={'mod': 'PubSubController', 'action': 'PublishMessage'})

            # Prepare the message as bytes
            message_data = json.dumps(message).encode("utf-8")

            # Publish the message to the specified Pub/Sub topic
            future = self.publisher.publish(
                self.topic_path, data=message_data
            )

            # Wait for the result of the publish (blocking or awaiting the result)
            future.result()  # Will raise an exception if publish fails

            logger.log_info(f"Successfully published message ", context={'mod': 'PubSubController', 'action': 'PublishMessageSuccess'})
        except Exception as e:
            # Handle error
            logger.log_error(f"Error publishing message: {str(e)}", context={'mod': 'PubSubController', 'action': 'PublishMessageError'})
