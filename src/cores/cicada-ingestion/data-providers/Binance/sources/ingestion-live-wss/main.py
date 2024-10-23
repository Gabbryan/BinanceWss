from controller_binance_wss import BinanceWSSClient
from src.commons.env_manager.env_controller import EnvController
from src.commons.notifications.notifications_controller import NotificationsController
from src.libs.utils.sys.threading.controller_threading import ThreadController

# Initialize the controller for the 'development' environment
env_controller = EnvController()
notifications_controller = NotificationsController('wss-binance')
if __name__ == "__main__":
    notifications_controller.send_process_start_message()
    # Initialize the controller
    thread_controller = ThreadController()
    symbols = env_controller.get_yaml_config('wss-binance', 'symbols')
    streams = env_controller.get_yaml_config('wss-binance', 'streams')
    # Create and add WebSocket clients for each symbol
    for symbol in symbols:
        for stream in streams:
            binance_client = BinanceWSSClient(symbol, stream=stream)
            # Adding the client and specifying 'connect' method to run in thread
            thread_controller.add_thread(binance_client, method_name="connect")

    # Start all threads (each running a WebSocket client)
    thread_controller.start_all()

    # Optionally, join all threads to stop them cleanly
    thread_controller.stop_all()
    notifications_controller.send_process_end_message()
