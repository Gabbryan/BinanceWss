import requests
import logging


class SlackChannel:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.token = "xoxe.xoxb-1-MS0yLTU5NTY3MTc4MTEzMjgtNzEzMzQwNzg1NTc2MS03MTMzNDA3OTYzMzc3LTcxMDYxODM2ODcyMjMtMGE1MzEwMTYxYzU3NmM1NDY3MDM0MzE3YTBhZjQxMmM1YzEzZjdkYjRiYjFlYmI5MGM0ZDJlNDYzNTNiNWY4YQ"

    def send_message(
        self, header: str, message: str, color: str = "#6a0dad", link: str = None
    ):
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

        logging.info(f"Payload for send_message: {payload}")

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

    def send_advanced_message(
        self,
        header: str,
        message: str,
        color: str = "#6a0dad",
        link: str = None,
        code: str = None,
        list_items: list = None,
    ):
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
        logging.info(f"Payload for send_advanced_message: {payload}")

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

    def upload_file(self, file_path: str, title: str):
        with open(file_path, "rb") as file_content:
            response = requests.post(
                "https://slack.com/api/files.upload",
                headers={"Authorization": f"Bearer {self.token}"},
                data={
                    "channels": "#dev",  # Change to your target channel
                    "title": title,
                },
                files={"file": file_content},
                timeout=5,
            )

        if response.status_code != 200 or not response.json().get("ok"):
            raise ValueError(
                f"File upload to Slack returned an error {response.status_code}, the response is:\n{response.text}"
            )

        file_info = response.json()["file"]
        return file_info["permalink"]

    def send_image(
        self, header: str, file_path: str, message: str = "", color: str = "#6a0dad"
    ):
        file_url = self.upload_file(file_path, header)
        self.send_message(header, f"{message}\n<{file_url}|View Image>", color)

        return {"status": "Image message sent successfully"}
