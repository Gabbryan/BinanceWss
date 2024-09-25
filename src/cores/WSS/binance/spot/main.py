from src.libs.tools.env_manager.env_controller import EnvController

# Initialize the controller for the 'development' environment
env_controller = EnvController(env="development")

# Access environment variables
db_host = env_controller.get_env("DB_HOST", "default-db-host")
api_key = env_controller.get_env("API_KEY", "default-api-key")

print(f"DB Host: {db_host}")
print(f"API Key: {api_key}")

# Access YAML configurations
app_name = env_controller.get_yaml_config('app', 'name')
app_debug = env_controller.get_yaml_config('app', 'debug', default_value=False)

print(f"App Name: {app_name}")
print(f"Debug Mode: {app_debug}")

# Get Google key path
google_key_path = env_controller.get_google_key_path()
if google_key_path:
    print(f"Google Key Path: {google_key_path}")
else:
    print("Google Key not found.")

# if __name__ == "__main__":
#     # Define the trading pairs (symbols)
#     symbols = ["btcusdt", "ethusdt", "ltcusdt"]
#
#     # Initialize the controller
#     thread_controller = ThreadController()
#
#     # Create and add WebSocket clients for each symbol
#     for symbol in symbols:
#         binance_client = BinanceWSSClient(symbol, stream="trade")
#         # Adding the client and specifying 'connect' method to run in thread
#         thread_controller.add_thread(binance_client, method_name="connect")
#
#     # Start all threads (each running a WebSocket client)
#     thread_controller.start_all()
#
#     # Optionally, join all threads to stop them cleanly
#     thread_controller.stop_all()
