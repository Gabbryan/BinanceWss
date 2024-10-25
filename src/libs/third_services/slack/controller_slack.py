from src.libs.third_services.slack.server_slack import SlackAPI

class SlackMessageController:
    def __init__(self, webhook_url: str):
        """
        Initialize SlackMessageController with a webhook URL for sending messages to Slack.

        :param webhook_url: The Slack webhook URL.
        """
        # Import LoggingController here to avoid circular import at the top level
        self.slack_api = SlackAPI(webhook_url)

    def format_message(self, header: str, message: str, color: str = "#6a0dad", link: str = None):
        """
        Format the message to be sent with a header, main message, and optionally a link.

        :param header: The header of the message.
        :param message: The main text of the message.
        :param color: The color of the message attachment.
        :param link: An optional link to include in the message.
        :return: A formatted payload ready to be sent to the Slack API.
        """
        if link:
            message = f"{message} <{link}>"

        payload = {
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": header, "emoji": True}},
                {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            ],
            "attachments": [{"color": color}],
        }

        return payload

    def send_slack_message(self, header: str, message: str, color: str = "#6a0dad", link: str = None):
        """
        Format and send a message to Slack using the webhook URL.

        :param header: The header of the message.
        :param message: The main text of the message.
        :param color: The color of the message attachment.
        :param link: An optional link to include in the message.
        """
        payload = self.format_message(header, message, color, link)
        try:
            response = self.slack_api.send_message(payload)
            return response
        except Exception as e:
            return None


if __name__ == "__main__":
    webhook_url = "https://hooks.slack.com/services/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"
    slack_controller = SlackMessageController(webhook_url)

    # Example usage
    response = slack_controller.send_slack_message(
        header="New Event",
        message="A new user has signed up.",
        color="#36a64f",
        link="https://example.com/user"
    )

    print(response)
