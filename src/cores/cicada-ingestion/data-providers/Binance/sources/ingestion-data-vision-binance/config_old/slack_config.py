import os

from dotenv import load_dotenv
from slack_package import init_slack, get_slack_decorators

load_dotenv()

# Slack Configuration
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Initialize Slack
slack_manager = init_slack(WEBHOOK_URL)
slack_decorators = get_slack_decorators()
