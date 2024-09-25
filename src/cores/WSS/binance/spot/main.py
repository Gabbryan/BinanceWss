from src.libs.CEX.binance.WSS.controller_binance_wss import BinanceWSSClient
from src.libs.tools.threading.controller_threading import ThreadController

if __name__ == "__main__":
    symbols = ["btcusdt", "ethusdt", "ltcusdt"]
    thread_controller = ThreadController()
    for symbol in symbols:
        binance_client = BinanceWSSClient(symbol, stream="trade")
        thread_controller.add_thread(binance_client, method_name="connect")
    thread_controller.stop_all()
