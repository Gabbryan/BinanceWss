import os
import time
from io import BytesIO
from datetime import datetime
import boto3
import pandas as pd
from dotenv import load_dotenv
import schedule
import emoji
from slack_package import init_slack, get_slack_decorators

# Charger les variables d'environnement
load_dotenv()

ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
REGION_NAME = os.getenv("AWS_REGION_NAME")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")

# Initialiser le client S3
s3_client = boto3.client(
    "s3",
    region_name=REGION_NAME,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
)


def list_files(prefix):
    """Lister les fichiers dans un bucket S3 avec un préfixe donné"""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    if "Contents" in response:
        return [item["Key"] for item in response["Contents"]]
    else:
        return []


def read_csv_from_s3(key):
    """Lire un fichier CSV depuis S3 et le convertir en DataFrame pandas"""
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
    return pd.read_csv(BytesIO(obj["Body"].read()))


def save_csv_to_s3(df, key):
    """Sauvegarder un DataFrame pandas en tant que fichier CSV sur S3"""
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())


slack_channel = init_slack(WEBHOOK_URL)
slack_decorators = get_slack_decorators()


@slack_decorators.notify_with_result(header="Aggregation Complete ", color="#36a64f")
def aggregate_and_notify():
    start_time = time.time()
    base_prefix = ""
    exchanges = ["binance_futures"]  # Ajoutez tous les échanges nécessaires
    timeframes = ["1mn"]  # Exemple de timeframes, ajoutez-en selon vos besoins

    # Dictionnaires pour stocker les DataFrames par symbole et par exchange
    symbol_exchange_dfs = {}

    # Liste pour collecter les chemins des fichiers générés
    generated_files = []

    for exchange in exchanges:
        for timeframe in timeframes:
            prefix = f"{base_prefix}{exchange}/{timeframe}/"
            files = list_files(prefix)

            for file_key in files:
                # Vérifiez que le chemin du fichier contient suffisamment d'éléments pour éviter l'IndexError
                try:
                    parts = file_key.split("/")
                    if len(parts) < 3 or "__" not in parts[2]:
                        print(f"Invalid file path structure: {file_key}")
                        continue

                    symbol = parts[2].split("_")[0]

                    df = read_csv_from_s3(file_key)
                    df["timeframe"] = timeframe
                    df["exchange"] = exchange
                    df["symbol"] = symbol

                    # Agrégation par symbole pour chaque exchange
                    if symbol not in symbol_exchange_dfs:
                        symbol_exchange_dfs[symbol] = {}
                    if exchange not in symbol_exchange_dfs[symbol]:
                        symbol_exchange_dfs[symbol][exchange] = pd.DataFrame()
                    symbol_exchange_dfs[symbol][exchange] = pd.concat(
                        [symbol_exchange_dfs[symbol][exchange], df], ignore_index=True
                    )

                except Exception as e:
                    print(f"Error processing file {file_key}: {e}")
                    continue

    # Sauvegarder les DataFrames agrégés sur S3 et collecter les chemins des fichiers générés
    for symbol, exchanges_dict in symbol_exchange_dfs.items():
        for exchange, df in exchanges_dict.items():
            file_path = f"pipeline_results/aggTrade/symbols/{symbol}/{exchange}/{symbol}_{exchange}_file.csv"
            save_csv_to_s3(df, file_path)
            generated_files.append(file_path)

    end_time = time.time()
    execution_time = end_time - start_time

    # Obtenir l'heure du prochain run prévu à partir de schedule
    next_run = schedule.next_run()

    # Créer le message avec les détails requis
    message = (
        f"*{emoji.emojize(':white_check_mark:')} Aggregation Complete*\n"
        f"*{emoji.emojize(':calendar:')} Date and Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"*{emoji.emojize(':page_with_curl:')} Generated Files:*\n"
        + "\n".join(f"> {file}" for file in generated_files)
        + "\n"
        f"*{emoji.emojize(':hourglass_flowing_sand:')} Execution Time:* {execution_time:.2f} seconds\n"
        f"*{emoji.emojize(':alarm_clock:')} Next Run:* {next_run.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"*{emoji.emojize(':bar_chart:')} Statistics:*\n"
        f"> - Number of files generated: {len(generated_files)}\n"
    )
    return message


def job():
    aggregate_and_notify()


# Planification de l'exécution toutes les 15 minutes
schedule.every(15).minutes.do(job)
schedule.run_all()

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
