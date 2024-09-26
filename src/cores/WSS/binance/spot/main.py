from src.commons.env_manager.env_controller import EnvController
from src.libs.financial.cryptos.CEX.binance.WSS.controller_binance_wss import BinanceWSSClient
from src.libs.tools.sys.threading.controller_threading import ThreadController

# Initialize the controller for the 'development' environment
env_controller = EnvController()

symbols = env_controller.get_yaml_config('symbols')
print(f"Symbols: {symbols}")
if __name__ == "__main__":
    # Define the trading pairs (symbols)
    symbols = ["btcusdt", "ethusdt", "ltcusdt"]

    # Initialize the controller
    thread_controller = ThreadController()

    # Create and add WebSocket clients for each symbol
    for symbol in symbols:
        binance_client = BinanceWSSClient(symbol, stream="trade")
        # Adding the client and specifying 'connect' method to run in thread
        thread_controller.add_thread(binance_client, method_name="connect")

    # Start all threads (each running a WebSocket client)
    thread_controller.start_all()

    # Optionally, join all threads to stop them cleanly
    thread_controller.stop_all()
