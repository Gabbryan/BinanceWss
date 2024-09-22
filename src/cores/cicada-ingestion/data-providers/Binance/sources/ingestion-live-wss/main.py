import asyncio
import json
import logging
import os

import aiohttp
from dotenv import load_dotenv
from google.cloud import pubsub_v1

load_dotenv()

# Configurer le client Pub/Sub
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
topic_id = os.getenv("PUBSUB_TOPIC_ID", "Binance-topic")

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Configurer le WebSocket Binance
BINANCE_WS_URL = "wss://stream.Binance.com:9443/ws"


# Fonction pour envoyer des messages à Pub/Sub
async def publish_message(symbol, message):
    try:
        future = publisher.publish(
            topic_path, data=json.dumps(message).encode("utf-8"), symbol=symbol
        )
        future.result()
        logging.info(f"Message publié pour {symbol}")
    except Exception as e:
        logging.error(f"Erreur lors de la publication du message : {str(e)}")


# Fonction pour traiter les données du marché
async def process_market_data(symbol, ws):
    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            await publish_message(symbol, data)


# Fonction pour gérer la connexion WebSocket
async def websocket_handler(symbol):
    uri = f"{BINANCE_WS_URL}/{symbol}@aggTrade"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(uri) as ws:
                    logging.info(f"WebSocket connecté pour {symbol}")
                    await process_market_data(symbol, ws)
        except Exception as e:
            logging.error(f"Erreur WebSocket pour {symbol} : {str(e)}")
        await asyncio.sleep(5)  # Attendre avant de se reconnecter


# Fonction principale
async def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logging.info("Service démarré...")

    symbols = ["btcusdt"]  # Ajoutez les paires de trading que vous voulez suivre
    tasks = [asyncio.create_task(websocket_handler(symbol)) for symbol in symbols]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
