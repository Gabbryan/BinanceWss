import requests
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("SlackAPI")

class SlackAPI:
    def __init__(self, webhook_url: str):
        """
        Initialize the SlackAPI with a webhook URL.

        :param webhook_url: The Slack webhook URL.
        """
        self.webhook_url = webhook_url
        logger.log_info("SlackAPI initialized with webhook URL.", context={'mod': 'SlackAPI', 'action': 'Init'})

    def send_message(self, payload: dict):
        """
        Send a message to Slack using a Webhook URL.

        :param payload: The formatted payload to send to the Slack API.
        :return: A dictionary with the status of the send operation.
        """
        try:
            response = requests.post(self.webhook_url, json=payload, timeout=5)
            if response.status_code != 200:
                raise ValueError(f"Request to Slack returned an error {response.status_code}, response:\n{response.text}")

            if response.text.strip() != "ok":
                raise ValueError(f"Request to Slack returned an unexpected response:\n{response.text}")

            logger.log_info("Message sent to Slack successfully.", context={'mod': 'SlackAPI', 'action': 'SendMessage'})
            return {"status": "Message sent successfully"}
        except Exception as e:
            logger.log_error(f"Error sending message to Slack: {e}", context={'mod': 'SlackAPI', 'action': 'SendMessageError'})
            raise
