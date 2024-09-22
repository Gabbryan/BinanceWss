import os
import random
import time
from datetime import datetime, timezone

import pandas as pd
import pytz
import requests
import schedule
from dotenv import load_dotenv

from slack_package import init_slack, get_slack_decorators

# Quotes for purge messages
purge_quotes = [
    "Remember all the good the Purge does.",
    "May God be with you all.",
    "Just remember all the good The Purge does.",
    "Blessed be our New Founding Fathers and America, a nation reborn.",
    "This is not a test. This is your emergency broadcast system announcing the commencement of the annual Purge.",
    "All crime, including murder, will be legal for 12 hours.",
    "Commencing at the siren, any and all crime, including murder, will be legal.",
    "Blessed be the New Founding Fathers for letting us Purge and cleanse our souls.",
    "It’s a night that is defining our country.",
    "The soul of our country is at stake. We must purge to cleanse ourselves of our sins.",
    "Tonight, we celebrate our right to purge.",
    "Purge and purify, America.",
]

# Quotes for preparation messages
preparation_quotes = [
    "Prepare for the night of your lives.",
    "Stock up on supplies and stay safe.",
    "Lock your doors and windows. The Purge is coming.",
    "Remember to arm yourself. Safety first.",
    "Stay indoors and trust no one.",
    "Make sure your security systems are armed.",
    "Double check your supplies. The night is long.",
    "Prepare your safe room. The Purge awaits.",
    "Stay vigilant. The Purge starts soon.",
    "Ensure all family members are accounted for.",
    "Stay alert. The siren will sound soon.",
    "Time to prepare. The Purge is almost here.",
]

# Load environment variables
load_dotenv()
BEARER_TOKEN = os.getenv("BEARER_TOKEN")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Initialize Slack with the webhook URL
init_slack(WEBHOOK_URL)

# Get Slack decorators after initialization
slack_decorators = get_slack_decorators()
slack_channel = slack_decorators.slack_channel


# Function to format time in a human-readable way
def format_time(seconds):
    minutes, seconds = divmod(seconds, 60)
    return f"{int(minutes)} minutes and {int(seconds)} seconds"


# Function to fetch trades
def fetch_trades(get_url, headers, midnight_utc_ms):
    """
    Fetch trades from the API below the given timestamp.
    """
    data = {
        "req": f"SELECT id, * FROM trade WHERE timestamp < {midnight_utc_ms} ORDER BY timestamp DESC;"  # nosec
    }
    print("Sending request to fetch trades...")
    response = requests.get(get_url, json=data, headers=headers, timeout=(5, 60))
    if response.status_code == 200:
        trades = response.json()
        if trades:
            print(f"Fetched {len(trades)} trades.")
            return trades
        else:
            print("No trades to fetch.")
            return []
    else:
        print(f"Failed to fetch trades: {response.status_code}")
        return []


# Decorated function to save trades to CSV
@slack_decorators.notify_with_result(
    header="cicada-ingestion has exported a new daily aggTrades file", color="#6a0dad"
)
def save_trades_to_csv(trades, directory, now):
    """
    Save trades to a CSV file in the specified directory with a timestamped filename.
    """
    os.makedirs(directory, exist_ok=True)
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(directory, f"trades_{timestamp_str}.csv")
    df = pd.DataFrame(trades)
    df.to_csv(filename, index=False)
    return f"The cicada-ingestion purge has commenced. Trades saved to `{filename}`."


# Decorated function to delete trades
@slack_decorators.notify_with_result(
    header="Daily batch delete operation on cicada-ingestion", color="#6a0dad"
)
def delete_trades(delete_url, headers, trade_ids):
    """
    Batch delete trades using the API.
    """
    print("Starting batch delete process...")
    batch_size = 100
    for i in range(0, len(trade_ids), batch_size):
        batch = trade_ids[i : i + batch_size]
        response = requests.delete(
            delete_url, json={"id": batch}, headers=headers, timeout=(5, 60)
        )
        if response.status_code == 200:
            response = response.json()
            nb_trades_delete = int(response["row_deleted"])
            print(f"Successfully deleted batch {nb_trades_delete}")
        else:
            print(
                f"Failed to delete batch {i // batch_size + 1}: {response.status_code}"
            )
    return f"The cicada-ingestion database has been cleansed. *{len(trade_ids)}* trades have been purged."


# Function to orchestrate the fetching, saving, and deleting of trades
def get_and_delete_trades():
    """
    Orchestrate the fetching, saving, and deleting of trades.
    """
    now = datetime.now(timezone.utc)
    midnight_utc = datetime(
        now.year, now.month, now.day, 0, 0, 0, 0, tzinfo=timezone.utc
    ).timestamp()
    midnight_utc_ms = int(midnight_utc * 1000)

    get_url = "https://cicada.bmcorp.fr/execute"
    delete_url = "https://cicada.bmcorp.fr/av/Trade"
    headers = {
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
    }

    quote = random.choice(purge_quotes)
    slack_channel.send_message(
        "🚨 Launching the purge of cicada-ingestion 🚨",
        f"*{quote}*\n\n> Commencing the purge sequence now. Stay vigilant.",
        color="#FF0000",
    )

    trades = fetch_trades(get_url, headers, midnight_utc_ms)
    if trades:
        directory = "archive/BTCUSDT/aggTrades"
        save_trades_to_csv(trades, directory, now)
        trade_ids = [trade["id"] for trade in trades]
        delete_trades(delete_url, headers, trade_ids)


# Function to wait until the next run
def wait_until_next_run():
    while True:
        next_run = schedule.next_run().replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        time_until_next_run = (next_run - now).total_seconds()

        if time_until_next_run > 43200:  # More than 12 hours
            sleep_time = 43200  # 12 hours
        elif time_until_next_run > 21600:  # More than 6 hours
            sleep_time = 21600  # 6 hours
        elif time_until_next_run > 14400:  # More than 4 hours
            sleep_time = 14400  # 4 hours
        elif time_until_next_run > 7200:  # More than 2 hours
            sleep_time = 7200  # 2 hours
        elif time_until_next_run > 3600:  # More than 1 hour
            sleep_time = 3600  # 1 hour
        elif time_until_next_run > 1800:  # More than 30 minutes
            sleep_time = 1800  # 30 minutes
        elif time_until_next_run > 900:  # More than 15 minutes
            sleep_time = 900  # 15 minutes
        elif time_until_next_run > 600:  # More than 10 minutes
            sleep_time = 300  # 5 minutes
        elif time_until_next_run > 300:  # More than 5 minutes
            sleep_time = 120  # 2 minutes
        elif time_until_next_run > 120:  # More than 2 minutes
            sleep_time = 60  # 1 minute
        elif time_until_next_run > 60:  # More than 1 minute
            sleep_time = 30  # 30 seconds
        elif time_until_next_run > 30:  # More than 30 seconds
            sleep_time = 15  # 15 seconds
        else:  # Less than 30 seconds
            sleep_time = 5  # 5 seconds

        preparation_quote = random.choice(preparation_quotes)
        slack_channel.send_message(
            " 🛠 Next run scheduled to purge cicada-ingestion database 🛠",
            f"*Next purge in {format_time(time_until_next_run)}. Sleeping for {format_time(sleep_time)}.*\n\n> _{preparation_quote}_",
            color="#FFA500",
        )
        time.sleep(sleep_time)

        if time_until_next_run <= 0:
            break


# Schedule tasks to run at midnight UTC
schedule.every().day.at("00:00", pytz.utc).do(get_and_delete_trades)
schedule.run_all()
print("Scheduler started.")

# Run the scheduler with adaptive sleeping
while True:
    schedule.run_pending()
    wait_until_next_run()
