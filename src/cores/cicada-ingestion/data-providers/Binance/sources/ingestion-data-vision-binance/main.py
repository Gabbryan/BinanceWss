from datetime import datetime

from controller_binance_data_vision import BinanceDataVision
from src.commons.env_manager.env_controller import EnvController
from src.commons.logs.logging_controller import LoggingController

# Initialize the logging controller
logger = LoggingController("MainDataVision")

env_controller = EnvController()

if __name__ == "__main__":
    # Fetch the start and end dates from the configuration
    initial_start_date = env_controller.get_yaml_config('Binance-data-vision', 'start_date')
    initial_end_date = env_controller.get_yaml_config('Binance-data-vision', 'end_date', default_value=datetime.now())

    # Log the configuration loading
    logger.log_info("Loaded start and end dates from configuration",
                    context={'mod': 'MainDataVision', 'action': 'LoadConfig'})

    # Initialize the BinanceDataVision instance
    binance_data_vision = BinanceDataVision()

    # Log the start of the download and processing operation
    logger.log_info("Starting data download and processing",
                    context={'mod': 'MainDataVision', 'action': 'StartProcess'})

    # Run the download and processing
    binance_data_vision.download_and_process_data(start_date=initial_start_date, end_date=initial_end_date)

    # Log the completion of the process
    logger.log_info("Data download and processing completed",
                    context={'mod': 'MainDataVision', 'action': 'EndProcess'})
