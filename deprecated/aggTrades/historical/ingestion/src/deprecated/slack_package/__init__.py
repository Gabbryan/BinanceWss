from .decorators import SlackDecorators
from .slack_channel import SlackChannel

__all__ = ["SlackChannel", "SlackDecorators", "init_slack", "get_slack_decorators"]

slack_channel = None
slack_decorators = None


def init_slack(webhook_url: str):
    global slack_channel, slack_decorators
    slack_channel = SlackChannel(webhook_url)
    slack_decorators = SlackDecorators(slack_channel)
    return slack_channel


def get_slack_decorators():
    global slack_decorators
    if slack_decorators is None:
        raise ValueError(
            "Slack decorators not initialized. Call init_slack(webhook_url) first."
        )
    return slack_decorators
