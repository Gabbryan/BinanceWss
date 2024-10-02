from datetime import datetime

from controller_binance_data_vision import BinanceDataVision
from src.commons.env_manager.env_controller import EnvController

env_controller = EnvController()
if __name__ == "__main__":
    initial_start_date = datetime(2024, 9, 28)
    initial_end_date = datetime.today()
    binance_data_vision = BinanceDataVision()
    binance_data_vision.download_and_process_data(start_date=initial_start_date, end_date=initial_end_date)
