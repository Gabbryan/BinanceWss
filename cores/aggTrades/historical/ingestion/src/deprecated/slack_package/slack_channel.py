import requests


class SlackChannel:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(
        self, header: str, message: str, color: str = "#6a0dad", link: str = None
    ):
        """
        Envoie un message stylisé dans le canal spécifié via un webhook.
        """
        # Construire le texte du message
        if link:
            message = f"{message} <{link}>"

        # Construire le payload avec Block Kit
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

        # Envoyer la requête à l'API Slack
        response = requests.post(self.webhook_url, json=payload, timeout=5)

        # Vérification du statut de la réponse
        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
            )

        # Vérifier si la réponse contient "ok"
        if response.text.strip() != "ok":
            raise ValueError(
                f"Request to Slack returned an unexpected response:\n{response.text}"
            )

        return {"status": "Message sent successfully"}

    def send_advanced_message(
        self,
        header: str,
        message: str,
        color: str = "#6a0dad",
        link: str = None,
        code: str = None,
        list_items: list = None,
    ):
        """
        Envoie un message avancé utilisant différentes fonctionnalités de Block Kit.
        """
        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": header, "emoji": True},
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": message}},
        ]

        if link:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"<{link}|Click here>"},
                }
            )

        if code:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"```\n{code}\n```"},
                }
            )

        if list_items:
            list_text = "\n".join([f"- {item}" for item in list_items])
            blocks.append(
                {"type": "section", "text": {"type": "mrkdwn", "text": list_text}}
            )

        payload = {"blocks": blocks, "attachments": [{"color": color}]}
        response = requests.post(self.webhook_url, json=payload, timeout=5)

        if response.status_code != 200:
            raise ValueError(
                f"Request to Slack returned an error {response.status_code}, the response is:\n{response.text}"
            )

        if response.text.strip() != "ok":
            raise ValueError(
                f"Request to Slack returned an unexpected response:\n{response.text}"
            )

        return {"status": "Advanced message sent successfully"}
