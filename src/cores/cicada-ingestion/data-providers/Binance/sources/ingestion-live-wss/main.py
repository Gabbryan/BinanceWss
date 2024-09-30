from controller_binance_wss import BinanceWSSClient
from src.commons.env_manager.env_controller import EnvController
from src.libs.utils.sys.threading.controller_threading import ThreadController

# Initialize the controller for the 'development' environment
env_controller = EnvController()

if __name__ == "__main__":
    # Initialize the controller
    thread_controller = ThreadController()
    symbols = env_controller.get_yaml_config('symbols')
    # Create and add WebSocket clients for each symbol
    for symbol in symbols:
        binance_client = BinanceWSSClient(symbol, stream=env_controller.get_env('stream', 'trade'))

        # Adding the client and specifying 'connect' method to run in thread
        thread_controller.add_thread(binance_client, method_name="connect")

    # Start all threads (each running a WebSocket client)
    thread_controller.start_all()

    # Optionally, join all threads to stop them cleanly
    thread_controller.stop_all()
