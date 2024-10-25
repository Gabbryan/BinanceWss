from controller_binance_wss import BinanceWSSClient
from src.commons.env_manager.env_controller import EnvController
from src.commons.notifications.notifications_controller import NotificationsController
from src.libs.utils.sys.threading.controller_threading import ThreadController

# Initialize environment and notification controllers
env_controller = EnvController()
notifications_controller = NotificationsController('wss-binance')

if __name__ == "__main__":
    # Send start message for the process
    notifications_controller.send_process_start_message()

    # Initialize the thread controller
    thread_controller = ThreadController()

    # Load symbols and streams from the YAML configuration
    symbols = env_controller.get_yaml_config('wss-binance', 'symbols')
    streams = env_controller.get_yaml_config('wss-binance', 'streams')

    # Add a WebSocket client in a separate thread for each symbol-stream pair
    for symbol in symbols:
        for stream in streams:
            try:
                binance_client = BinanceWSSClient(symbol, stream=stream)
                thread_controller.add_thread(binance_client, method_name="connect")
            except Exception as e:
                notifications_controller.send_error_notification(
                    f"Error initializing WebSocket client for {symbol} on {stream}: {e}", "BinanceWSSClient"
                )

    # Start all threads to establish WebSocket connections
    thread_controller.start_all()

    # Cleanly stop and join all threads once they are done
    thread_controller.stop_all()

    # Send end message for the process
    notifications_controller.send_process_end_message()
