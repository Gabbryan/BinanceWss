import logging
import os
import time

import pandas as pd
from dotenv import load_dotenv
from gcs_module import gcsModule
from github import Github, GithubException
from slack_package import init_slack, get_slack_decorators
from tqdm import tqdm  # Import tqdm for progress bars

from config import BUCKET_NAME

# Load environment variables from .env file
load_dotenv()

# Initialize Slack
webhook_url = os.getenv("WEBHOOK_URL")
slack_channel = init_slack(webhook_url)
slack_decorators = get_slack_decorators()

# Initialize GCS module
gcs_module = gcsModule(bucket_name=BUCKET_NAME)

# Authenticate to GitHub (use a personal access token if necessary)
g = Github(
    "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"
)

# Define a dictionary of cryptocurrencies with their repositories
crypto_repos = {
    "avalanche": ["ava-labs/subnet-evm", "ava-labs/avalanchego"],
    "bitcoin": ["bitcoin/bitcoin"],
    "ethereum": ["ethereum/go-ethereum", "ethereum/solidity", "ethereum/evmone"],
    "binance_coin": ["bnb-chain/reth", "bnb-chain/bsc", "bnb-chain/revm"],
    "solana": [
        "solana-labs/solana-program-library",
        "solana-labs/rbpf",
        "solana-labs/solana-web3.js",
    ],
    "ripple": ["ripple/opensource.ripple.com", "ripple/explorer"],
    "ton": ["ton-blockchain/ton", "ton-blockchain/mytonctrl"],
    "dogecoin": ["dogecoin/dogecoin"],
    "cardano": ["cardano-foundation/cardano-wallet"],
    "tron": ["tronprotocol/java-tron"],
}

# Validate repositories by checking if they exist
for crypto, repos in list(crypto_repos.items()):
    valid_repos = []
    for repo_name in repos:
        try:
            logging.info(
                f"Validating repository '{repo_name}' for cryptocurrency '{crypto}'"
            )
            # Attempt to get the repository
            g.get_repo(repo_name)
            valid_repos.append(repo_name)
        except GithubException as e:
            logging.warning(
                f"Repository '{repo_name}' for cryptocurrency '{crypto}' not found: {str(e)}"
            )

    # Update the list of repositories for the cryptocurrency with only valid ones
    if valid_repos:
        crypto_repos[crypto] = valid_repos
    else:
        del crypto_repos[crypto]  # Remove the cryptocurrency if no valid repositories

# Send Slack start message
start_time = time.time()
total_files = 0
total_size = 0


def send_start_message(symbols, data_name):
    message = (
        f":satellite_antenna: *{data_name.capitalize()} Data Processing Initiated*\n\n"
        f"🚀 *Task Overview:*\n"
        f"- *Number of Cryptocurrencies*: `{len(symbols)}`\n"
        f"- *Cryptocurrencies in Focus*:\n"
    )
    for symbol in symbols:
        message += f"  • `{symbol.capitalize()}`\n"
    message += (
        "\nPlease wait while we fetch and process the latest data from GitHub repositories. "
        "Progress updates will follow shortly!"
    )
    slack_channel.send_message(
        ":satellite_antenna: GitHub Data Processing Initiated", message, "#36a64f"
    )


def send_end_message(start_time, total_files, total_size, data_name):
    end_time = time.time()
    elapsed_time = end_time - start_time
    message = (
        f":trophy: *{data_name.capitalize()} Data Processing Completed*\n\n"
        f"⏳ *Total Time Taken*: `{elapsed_time / 60:.2f} minutes`\n"
        "All specified GitHub repositories have been successfully processed. "
        "The data is now available in the designated storage location. Excellent work, team! :clap:"
    )
    slack_channel.send_message(
        ":trophy: GitHub Data Processing Completed", message, "#36a64f"
    )


def fetch_and_process_commits(repo, crypto, repo_name):
    commits = []
    for commit in tqdm(repo.get_commits(), desc="Fetching commits", unit="commit"):
        # Extract relevant commit information including the date
        commits.append(
            {
                "sha": commit.sha,
                "author": commit.commit.author.name if commit.commit.author else None,
                "message": commit.commit.message,
                "date": commit.commit.last_modified_datetime,
            }
        )

    # Convert to DataFrame
    commits_df = pd.DataFrame(commits)

    # Check if the date column is present and not null
    if "date" not in commits_df.columns or commits_df["date"].isnull().all():
        logging.error(
            f"Column 'date' not found in data for symbol '{crypto}' and repository '{repo_name}'."
        )
        return None

    # Convert date to datetime and set as index
    commits_df["date"] = pd.to_datetime(commits_df["date"], errors="coerce")
    return commits_df


def fetch_and_process_issues(repo):
    issues = []
    for issue in tqdm(
        repo.get_issues(state="all"), desc="Fetching issues", unit="issue"
    ):
        issues.append(
            {
                "id": issue.id,
                "title": issue.title,
                "state": issue.state,
                "created_at": issue.created_at,
                "closed_at": issue.closed_at,
                "comments": issue.comments,
                "user": issue.user.login,
            }
        )
    issues_df = pd.DataFrame(issues)
    issues_df["created_at"] = pd.to_datetime(issues_df["created_at"])
    issues_df.set_index("created_at", inplace=True)
    return issues_df


def fetch_and_process_pull_requests(repo):
    pulls = []
    for pr in tqdm(
        repo.get_pulls(state="all"), desc="Fetching pull requests", unit="pull request"
    ):
        pulls.append(
            {
                "id": pr.id,
                "title": pr.title,
                "state": pr.state,
                "created_at": pr.created_at,
                "closed_at": pr.closed_at,
                "merged_at": pr.merged_at,
                "user": pr.user.login,
            }
        )
    pulls_df = pd.DataFrame(pulls)
    pulls_df["created_at"] = pd.to_datetime(pulls_df["created_at"])
    pulls_df.set_index("created_at", inplace=True)
    return pulls_df


send_start_message(list(crypto_repos.keys()), "GitHub Data")
# Process each cryptocurrency and its repositories
for crypto, repos in crypto_repos.items():
    for repo_name in repos:
        print(f"Processing repository: {repo_name} for cryptocurrency: {crypto}")
        repo = g.get_repo(repo_name)

        # Fetch and process commits
        commits_df = fetch_and_process_commits(repo, crypto, repo_name)
        if commits_df is not None:
            gcs_module.aggregate_and_upload(commits_df, crypto, repo_name, "commits")

        # Fetch and process issues
        issues_df = fetch_and_process_issues(repo)
        if not issues_df.empty:
            gcs_module.aggregate_and_upload(issues_df, crypto, repo_name, "issues")

        # Fetch and process pull requests
        pulls_df = fetch_and_process_pull_requests(repo)
        if not pulls_df.empty:
            gcs_module.aggregate_and_upload(
                pulls_df, crypto, repo_name, "pull_requests"
            )

# End processing
send_end_message(start_time, total_files, total_size, "GitHub Data")
