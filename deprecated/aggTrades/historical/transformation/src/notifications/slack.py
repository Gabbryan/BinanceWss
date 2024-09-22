import logging
import requests


class SlackChannel:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_message(self, title, message):
        payload = {"text": f"*{title}*\n{message}"}
        response = requests.post(self.webhook_url, json=payload, timeout=5)
        if response.status_code != 200:
            logging.error(f"Failed to send message to Slack: {response.text}")


def get_slack_decorators():
    def slack_notifier(func):
        def wrapper(*args, **kwargs):
            slack_channel = args[0]
            slack_channel.send_message("Start", f"Starting {func.__name__}")
            result = func(*args, **kwargs)
            slack_channel.send_message("End", f"Finished {func.__name__}")
            return result

        return wrapper

    return {"slack_notifier": slack_notifier}
