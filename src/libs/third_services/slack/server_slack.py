import requests


class SlackAPI:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(self, payload: dict):
        """
        Envoie un message à Slack via une URL Webhook.

        :param payload: Le payload formaté à envoyer à l'API Slack.
        :return: Un dictionnaire avec le statut de l'envoi.
        """
        response = requests.post(self.webhook_url, json=payload, timeout=5)

        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
            )

        if response.text.strip() != "ok":
            raise ValueError(
                f"Request to Slack returned an unexpected response:\n{response.text}"
            )

        return {"status": "Message sent successfully"}
