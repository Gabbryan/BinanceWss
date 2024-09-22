import time

import schedule
from Cicada.deprecated.aggTrades.historical.transformation.src.utils.utils import (
    check_disk_space,
    clear_temp_files,
)
from cores.aggTrades.historical.transformation.src.deprecated.main_pipeline import (
    daily_update,
)

# Schedule Tasks
schedule.every().hour.do(check_disk_space)
schedule.every().day.at("03:00").do(clear_temp_files)
schedule.every().day.at("10:00").do(daily_update)

# Run Scheduled Tasks
schedule.run_all()
while True:
    schedule.run_pending()
    time.sleep(1)
