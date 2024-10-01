from src.libs.third_services.slack.server_slack import SlackAPI

class SlackMessageController:
    def __init__(self, webhook_url: str):
        self.slack_api = SlackAPI(webhook_url)

    def format_message(self, header: str, message: str, color: str = "#6a0dad", link: str = None):
        """
        Formate le message à envoyer avec un en-tête, un message principal et éventuellement un lien.

        :param header: L'en-tête du message.
        :param message: Le texte principal du message.
        :param color: La couleur de l'attachement du message.
        :param link: Un lien facultatif à inclure dans le message.
        :return: Un payload formaté prêt à être envoyé à l'API Slack.
        """
        if link:
            message = f"{message} <{link}>"

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": header, "emoji": True},
                },
                {"type": "section", "text": {"type": "mrkdwn", "text": message}},
            ],
            "attachments": [{"color": color}],
        }

        return payload

    def send_slack_message(self, header: str, message: str, color: str = "#6a0dad", link: str = None):
        """
        Formate et envoie un message à Slack via le channel défini dans server_slack.py

        :param header: L'en-tête du message.
        :param message: Le texte principal du message.
        :param color: La couleur de l'attachement du message.
        :param link: Un lien facultatif à inclure dans le message.
        """
        payload = self.format_message(header, message, color, link)
        return self.slack_api.send_message(payload)


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
